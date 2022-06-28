/*************************************************************************
 * odil - Copyright (C) Universite de Strasbourg
 * Distributed under the terms of the CeCILL-B license, as published by
 * the CEA-CNRS-INRIA. Refer to the LICENSE file or to
 * http://www.cecill.info/licences/Licence_CeCILL-B_V1-en.html
 * for details.
 ************************************************************************/

#include "odil/dul/Transport.h"

#include <algorithm>
#include <boost/asio.hpp>
#include <boost/date_time.hpp>
#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <string>

#include "boost/asio/error.hpp"
#include "boost/exception/exception.hpp"
#include "boost/system/detail/error_code.hpp"
#include "boost/system/system_error.hpp"
#include "odil/Exception.h"
#include "odil/logging.h"

namespace odil {

namespace dul {

struct HandlerPromise {
  HandlerPromise() : p(), f(this->p.get_future()) {}

  ~HandlerPromise() {
    set_value(boost::asio::error::make_error_code(
        boost::asio::error::basic_errors::operation_aborted));
  }

  void set_value(const boost::system::error_code& ec) {
    using namespace std::literals;
    // This trick allows to check if future has already been set or not, there
    // is no standard API for this.
    if (f.valid() && f.wait_for(std::chrono::milliseconds(0)) ==
                         std::future_status::timeout) {
      p.set_value(ec);
    }
  }

  std::promise<boost::system::error_code> p;
  std::future<boost::system::error_code> f;
};

// TODO put it in a shared file
template <class InputIt, class T>
bool is_in_range(InputIt first, InputIt last, const T& value) {
  return std::find(first, last, value) != last;
}

#define MAKE_ERROR(error_name) \
  boost::asio::error::make_error_code(boost::asio::error::error_name)

static boost::system::error_code CONNECTION_CLOSED_INDICATOR[]{
    MAKE_ERROR(connection_aborted), MAKE_ERROR(broken_pipe),
    MAKE_ERROR(connection_refused), MAKE_ERROR(connection_reset),
    MAKE_ERROR(host_unreachable),   MAKE_ERROR(network_down),
    MAKE_ERROR(network_reset),      MAKE_ERROR(network_unreachable),
    MAKE_ERROR(not_connected),      MAKE_ERROR(not_socket),
    MAKE_ERROR(shut_down),          MAKE_ERROR(eof),
};
#undef MAKE_ERROR

void waitHandlerResult(const std::shared_ptr<HandlerPromise>& handler_promise,
                       Transport::duration_type timeout,
                       boost::system::error_code& ec) {
  std::future_status wait_result = std::future_status::ready;
  if (timeout.is_pos_infinity() || timeout.is_neg_infinity()) {
    handler_promise->f.wait();
  } else {
    auto wait_duration =
        std::chrono::microseconds(timeout.total_microseconds());
    wait_result = handler_promise->f.wait_for(wait_duration);
  }

  if (wait_result != std::future_status::ready) {
    throw Exception("TCP time out");
  }

  ec = handler_promise->f.get();
}

void waitHandlerResult(const std::shared_ptr<HandlerPromise>& handler_promise,
                       Transport::duration_type timeout) {
  boost::system::error_code ec;
  waitHandlerResult(handler_promise, timeout, ec);
  if (ec) throw boost::system::system_error(ec);
}

Transport ::Transport()
    : _service(),
      _socket(nullptr),
      _timeout(boost::posix_time::pos_infin),
      _deadline(_service) {
  _service_work_guard =
      std::make_unique<boost::asio::io_context::work>(_service);
  _io_service_thread = std::thread([this]() { _service.run(); });
  // Nothing else
}

Transport ::~Transport() {
  if (this->is_open()) {
    this->close();
  }
  _service_work_guard = nullptr;
  _service.stop();
  _io_service_thread.join();
}

boost::asio::io_service const& Transport ::get_service() const {
  return this->_service;
}

boost::asio::io_service& Transport ::get_service() { return this->_service; }

std::shared_ptr<Transport::Socket const> Transport ::get_socket() const {
  return this->_socket;
}

std::shared_ptr<Transport::Socket> Transport ::get_socket() {
  std::lock_guard<std::mutex> l(_socket_mutex);
  std::shared_ptr<Transport::Socket> socket = this->_socket;
  return socket;
}

Transport::duration_type Transport ::get_timeout() const {
  return this->_timeout;
}

void Transport ::set_timeout(duration_type timeout) {
  this->_timeout = timeout;
}

bool Transport ::is_open() const {
  auto socket = this->get_socket();
  return (socket != nullptr && socket->is_open());
}

void Transport ::connect(Socket::endpoint_type const& peer_endpoint) {
  if (this->is_open()) {
    throw Exception("Already connected");
  }

  auto socket = std::make_shared<Socket>(this->_service);

  auto handler_promise = std::make_shared<HandlerPromise>();

  socket->async_connect(peer_endpoint,
                        [handler_promise](boost::system::error_code const& e) {
                          handler_promise->set_value(e);
                        });

  waitHandlerResult(handler_promise, this->get_timeout());
  this->_set_socket(socket);
}

void Transport ::receive(Socket::endpoint_type const& endpoint) {
  if (this->is_open()) {
    throw Exception("Already connected");
  }

  auto socket = std::make_shared<Socket>(this->_service);
  this->_acceptor = std::make_shared<boost::asio::ip::tcp::acceptor>(
      this->_service, endpoint);
  boost::asio::socket_base::reuse_address option(true);
  this->_acceptor->set_option(option);

  auto handler_promise = std::make_shared<HandlerPromise>();

  this->_acceptor->async_accept(
      *socket, [handler_promise](boost::system::error_code const& e) {
        handler_promise->set_value(e);
      });

  waitHandlerResult(handler_promise, this->get_timeout());

  this->_set_socket(socket);

  this->_acceptor = nullptr;
}

void Transport ::close() {
  if (this->_acceptor && this->_acceptor->is_open()) {
    this->_acceptor->close();
    this->_acceptor = nullptr;
  }
  if (this->is_open()) {
    auto socket = this->get_socket();
    try {
      socket->shutdown(boost::asio::ip::tcp::socket::shutdown_both);
    }
    // Only used to prevent uncatchable error on WIN32 platforms and in
    // wrappers.
    catch (std::exception& e) {
      ODIL_LOG(debug) << "Could not shut down transport: " << e.what();
    } catch (...) {
      ODIL_LOG(debug) << "Could not shut down transport (unknown exception)";
    }
    socket->close();
    this->_set_socket(nullptr);
  }
}

std::string Transport ::read(std::size_t length) {
  auto socket = this->get_socket();
  if (!this->is_open()) {
    throw Exception("Not connected");
  }

  std::string data(length, 'a');
  auto handler_promise = std::make_shared<HandlerPromise>();
  boost::asio::async_read(
      *socket, boost::asio::buffer(&data[0], data.size()),
      [handler_promise](boost::system::error_code const& e, std::size_t) {
        handler_promise->set_value(e);
      });

  boost::system::error_code ec;
  waitHandlerResult(handler_promise, this->get_timeout(), ec);
  if (ec) {
    if (is_in_range(
            CONNECTION_CLOSED_INDICATOR,
            CONNECTION_CLOSED_INDICATOR + sizeof(CONNECTION_CLOSED_INDICATOR),
            ec)) {
      this->close();
      throw TransportClosed();
    } else {
      throw boost::system::system_error(ec);
    }
  }

  return data;
}

void Transport ::write(std::string const& data) {
  auto socket = this->get_socket();
  if (!this->is_open()) {
    throw Exception("Not connected");
  }

  auto handler_promise = std::make_shared<HandlerPromise>();

  boost::asio::async_write(
      *socket, boost::asio::buffer(data),
      [handler_promise](boost::system::error_code const& e, std::size_t) {
        handler_promise->set_value(e);
      });

  waitHandlerResult(handler_promise, this->get_timeout());
}

void Transport ::_set_socket(std::shared_ptr<Socket> socket) {
  std::lock_guard<std::mutex> l(this->_socket_mutex);
  this->_socket = socket;
}

// void Transport ::_start_deadline(Source& source,
//                                  boost::system::error_code& error) {
//   auto const canceled = this->_deadline.expires_from_now(this->_timeout);
//   if (canceled != 0) {
//     throw Exception("TCP timer started with pending operations");
//   }

//   this->_deadline.async_wait(
//       [&source, &error](boost::system::error_code const& e) {
//         source = Source::TIMER;
//         error = e;
//       });
// }

// void Transport ::_stop_deadline() {
//   this->_deadline.expires_at(boost::posix_time::pos_infin);
// }

// void Transport ::_run(Source& source, boost::system::error_code& error) {
//   // WARNING: it seems that run_one runs a *simple* operation, not a
//   // *composed* operation, as is done by async_read/async_write
//   while (source == Source::NONE) {
//     auto const ran = this->_service.run_one();
//     if (ran == 0) {
//       throw Exception("No operations ran");
//     }
//     this->_service.reset();
//   }

//   if (source == Source::OPERATION) {
//     if (error) {
//       throw Exception("Operation error: " + error.message());
//     }

//     source = Source::NONE;
//     this->_stop_deadline();

//     while (source == Source::NONE) {
//       auto const polled = this->_service.poll_one();
//       if (polled == 0) {
//         throw Exception("No operations polled");
//       }
//       this->_service.reset();
//     }

//     if (source != Source::TIMER) {
//       throw Exception("Unknown event");
//     } else if (error != boost::asio::error::operation_aborted) {
//       throw Exception("TCP timer error: " + error.message());
//     }
//   } else if (source == Source::TIMER) {
//     throw Exception("TCP time out");
//   } else {
//     throw Exception("Unknown source");
//   }
// }

TransportClosed ::TransportClosed() : Exception("Transport Closed") {}

}  // namespace dul

}  // namespace odil
