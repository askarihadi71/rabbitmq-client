# RabbitMQ Client and Thread-Safe Wrapper

This repository contains a robust and flexible Python solution for interacting with RabbitMQ, designed to handle both asynchronous and synchronous application needs. It comprises two main components: `RabbitMQClient` (the asynchronous core) and `ThreadSafeRabbitMQWrapper` (a wrapper for thread-safe access).

## Table of Contents

1. [Introduction](#1-introduction)

2. [RabbitMQClient - The Asynchronous Core](#2-rabbitmqclient---the-asynchronous-core)

   * [Key Features](#21-key-features)

   * [Decorators](#22-decorators)

   * [Key Methods](#23-key-methods)

3. [ThreadSafeRabbitMQWrapper - Bridging Async and Sync](#3-threadsaferabbitmqwrapper---bridging-async-and-sync)

   * [Why It's Needed: Thread Safety with Asyncio](#31-why-its-needed-thread-safety-with-asyncio)

   * [Mechanism](#32-mechanism)

   * [Key Methods](#33-key-methods)

4. [How They Work Together: A Unified Flow](#4-how-they-work-together-a-unified-flow)

5. [Usage Examples](#5-usage-examples)

   * [Publisher-Only Scenario](#51-publisher-only-scenario)

   * [Consumer-Only Scenario](#52-consumer-only-scenario)

   * [Combined Publisher and Consumer Scenario](#53-combined-publisher-and-consumer-scenario)

6. [Benefits of this Architecture](#6-benefits-of-this-architecture)

## 1. Introduction

Working with RabbitMQ in Python, especially in applications that mix synchronous and asynchronous code, can present challenges, particularly around managing connections, channels, and thread safety. This codebase offers a two-tiered solution:

* **`RabbitMQClient`**: This is the core, asynchronous RabbitMQ client built on `aio-pika`. It handles the direct interaction with RabbitMQ, providing robust features like automatic reconnection, channel management, publisher confirms, and Dead Letter Exchange (DLX) support for message retries. It is designed to run within a single `asyncio` event loop.

* **`ThreadSafeRabbitMQWrapper`**: This class acts as a wrapper around the `RabbitMQClient`. Its primary purpose is to make the asynchronous `RabbitMQClient` safely accessible from synchronous code or from multiple different threads within a Python application. It achieves this by running the `RabbitMQClient` in its own dedicated `asyncio` thread and using standard Python `queue.Queue` objects for inter-thread communication.

This architecture ensures that `aio-pika`'s asynchronous operations are confined to their dedicated event loop, preventing common `asyncio` thread-safety issues, while still offering a convenient, blocking interface for other parts of your application.

## 2. RabbitMQClient - The Asynchronous Core

The `RabbitMQClient` is a powerful, self-contained client capable of both publishing and consuming messages with high reliability.

### 2.1 Key Features:

* **Asynchronous Operations**: Leverages `asyncio` and `aio-pika` for non-blocking I/O, allowing efficient handling of many concurrent RabbitMQ interactions.

* **Automatic Reconnection (`connect` method)**: Implements robust reconnection logic with exponential backoff to recover from network outages or RabbitMQ server restarts. It automatically re-establishes connections, channels, exchanges, and queues.

* **Channel Management (`_create_channel` method)**: Ensures a healthy channel is always available for operations.

* **Publisher Confirms**: Supports reliable publishing where the broker acknowledges message receipt, providing a higher guarantee of delivery.

* **Dead Letter Exchange (DLX) and Retry Mechanism (`_setup_retry_dlx`)**: For consumers, it provides an advanced fault-tolerance mechanism:

  * Messages that fail processing are automatically re-published to a "retry queue."

  * The retry queue has a Time-To-Live (TTL), after which messages are dead-lettered back to the original queue for reprocessing.

  * After a configurable number of retries (`max_retries`), messages are sent to a "Dead Letter Queue" (DLQ) for manual inspection, preventing endlessly failing messages from clogging the system.

* **Unified Publisher and Consumer**: A single instance of `RabbitMQClient` can be configured to act as both a publisher (sending messages to an exchange) and a consumer (receiving messages from a queue).

* **Configurable Exchanges and Queues**: Allows defining exchange names, types (topic, direct, fanout), durability, and consumer queue properties.

* **Comprehensive Logging**: Integrates detailed logging for connection status, message flow, and error handling, aiding in debugging and monitoring.

* **Celery-Compatible Task Publishing**: Allows publishing messages that Celery workers can automatically pick up, as if they were sent using `task.delay(...)`. By simulating Celery’s AMQP protocol, the client can publish structured task messages with proper headers and payloads, enabling tight integration with Celery workers.


### 2.2 Decorators:

The `RabbitMQClient` uses two custom decorators to ensure operational readiness:

* **`@requires_connection`**: This decorator automatically checks if a RabbitMQ connection is active before executing the decorated `async` method. If not, it attempts to reconnect.

* **`@requires_channel`**: Similar to `requires_connection`, this decorator ensures an active channel is available. If the channel is closed or `None`, it attempts to create a new one.

### 2.3 Key Methods:

* **`__init__(...)`**: Initializes the client with connection details, reconnection settings, prefetch count (for consumers), and separate configurations for consumer and publisher aspects (queue names, exchange names/types, routing keys, DLX settings). It's designed to be flexible; you can configure it for publisher-only, consumer-only, or both by providing/omitting the relevant consumer parameters.

* **`connect() -> None`**: Establishes a robust connection to the RabbitMQ broker. Employs exponential backoff for retries in case of connection failures. Also triggers the creation of a channel and declaration of the publisher exchange once connected.

* **`_on_connection_close(sender, exc)`**: An internal callback triggered when the underlying `aio-pika` connection closes (e.g., due to network issues or broker shutdown). It logs the event and automatically initiates the `connect()` method to attempt reconnection.

* **`_create_channel() -> None`**: An internal method to create an `aio-pika` channel from the established connection. Sets the `prefetch_count` (how many unacknowledged messages a consumer can receive at once). Enables publisher confirms if `publisher_confirm` is `True`.

* **`_declare_publisher_exchange() -> None`**: An internal method that declares the publisher's main exchange based on `publisher_exchange_name`, `publisher_exchange_type`, and `publisher_exchange_durable`. This exchange is where messages will be sent.

* **`publish(message: bytes, routing_key: str, exchange_name: Optional[str] = None, ...)`**: The primary method for sending messages. Takes the `message` body (as bytes) and a `routing_key`. Optionally allows specifying a different `exchange_name` for dynamic publishing to exchanges other than the default. Handles potential `ChannelClosed` or `ConnectionClosed` exceptions during publishing.

* **`publish_celery_task(task_name: str, routing_key: Optional[str] = None, args: list = [], kwargs: dict = {}, ...)`**:
  Publishes a Celery-compatible task message to a configured RabbitMQ exchange. This method formats the message in a way that Celery workers can consume and execute the task natively. Requires `celery_publish_exchange` and `celery_publish_routing_key` to be configured. The exchange type should typically be `ExchangeType.DIRECT`, unless your Celery worker is explicitly configured otherwise.


* **`_setup_retry_dlx() -> None`**: Internal method responsible for setting up the Dead Letter Exchange (DLX) infrastructure. Declares the dead-letter queue (DLQ) and the retry queue. The retry queue is configured to send messages back to the original consumer queue after a TTL.

* **`start_consuming(message_callback: Callable)`**: Initiates message consumption. Declares the consumer queue (durable by default, or exclusive/auto-delete if `consumer_queue_name` is `None`). Binds the queue to the specified consumer exchange with the given routing key. Starts an internal `_start_consuming_loop` as an `asyncio.Task`. **Important**: The `message_callback` provided should focus solely on processing the message's content. Acknowledgment (ACK/NACK) is handled internally by the client using `message.process()`.

* **`_start_consuming_loop() -> None`**: The internal asynchronous loop that fetches messages from the consumer queue. It wraps each message processing in `async with message.process():`, which automatically acknowledges the message upon successful execution of `message_callback`, or handles retries/DLQ routing if `message_callback` raises an exception.

* **`stop_consuming() -> None`**: Gracefully stops the active message consumer task by cancelling the underlying `asyncio` task.

* **`close() -> None`**: Shuts down the RabbitMQ client, stopping consumption and closing the channel and connection gracefully.

## 3. ThreadSafeRabbitMQWrapper - Bridging Async and Sync

The `ThreadSafeRabbitMQWrapper` is designed for applications where you need to interact with RabbitMQ from synchronous code or from multiple Python threads (e.g., in a traditional web framework, a desktop application, or a multi-threaded daemon).

### 3.1 Why It's Needed: Thread Safety with Asyncio

`aio-pika` and `asyncio` are built around a single-threaded event loop. You cannot directly call `await` functions (like `_client.publish()`) from threads other than the one running the `asyncio` event loop. Attempting to do so will lead to runtime errors or unpredictable behavior.

The `ThreadSafeRabbitMQWrapper` solves this by creating a dedicated thread to host the `asyncio` event loop and the `RabbitMQClient` instance. Communication between your main application thread(s) and this dedicated asyncio thread happens via standard, thread-safe Python `queue.Queue` objects.

### 3.2 Mechanism:

1. **Dedicated Asyncio Thread**: When `wrapper.start()` is called, it spawns a new `threading.Thread`. Inside this new thread, an independent `asyncio` event loop is created, and the `RabbitMQClient` instance is instantiated within this loop. All direct `aio-pika` calls (connection, channel management, publishing, consuming) happen exclusively within this dedicated thread's event loop.

2. **Publisher Queue (`publish_queue`)**:

   * When your main application thread calls `wrapper.publish_message()`, the message data is immediately placed into `self.publish_queue` (a `queue.Queue`). This operation is non-blocking for your main thread.

   * The dedicated asyncio thread continuously monitors `self.publish_queue`. When it finds a message, it retrieves it and then safely calls `await self._client.publish()` within its own event loop.

3. **Incoming Messages Queue (`incoming_messages_queue`)**:

   * When the `RabbitMQClient` (running in the dedicated asyncio thread) consumes a message, its internal `_consumer_callback` processes it and then places the relevant message data (body, routing key) into `self.incoming_messages_queue`.

   * Your main application thread can then call `wrapper.get_consumed_message()`, which is a **blocking** call that waits for messages to appear in this queue.

This queue-based communication pattern effectively isolates the asynchronous RabbitMQ logic while providing a synchronous, thread-safe interface to the rest of your application.

### 3.3 Key Methods:

* **`__init__(...)`**: Takes the same comprehensive configuration parameters as `RabbitMQClient` because it configures the internal `RabbitMQClient` instance it will manage. Initializes the `threading.Event` (`_stop_event`) for graceful shutdown and the two `queue.Queue` instances for inter-thread communication.

* **`_run_asyncio_loop()`**: The target function for the dedicated thread. Creates a new `asyncio` event loop and sets it as the current loop for this thread. Instantiates the `RabbitMQClient` within this loop. Connects the client and, if configured, starts its internal consuming loop. Contains the main loop that continuously checks `self.publish_queue` for messages to publish and processes them using the `_client`.

* **`start()`**: Spawns and starts the dedicated `threading.Thread` that runs the `_run_asyncio_loop`.

* **`stop()`**: Sets an internal stop event (`_stop_event`) to signal the dedicated thread to shut down. Waits for the dedicated thread to complete its graceful shutdown using `thread.join()`.

* **`publish_message(message: str, routing_key: str)`**: The public, thread-safe method for publishing messages from any thread. It places the message and routing key into the `self.publish_queue` for the asyncio thread to pick up. This call is non-blocking.

* **`publish_celery_task(task_name: str, routing_key: Optional[str] = None, args: list = [], kwargs: dict = {})`**:
  A thread-safe method to enqueue a Celery task for asynchronous publishing. This is useful when your synchronous code wants to trigger Celery tasks without importing Celery or using `task.delay(...)`. Under the hood, it formats the task message and routes it through the RabbitMQ client in the dedicated asyncio thread.

* **`get_consumed_message(timeout: Optional[float] = None) -> Optional[Dict[str, Any]]`**: The public, thread-safe method for retrieving consumed messages. It attempts to get a message from `self.incoming_messages_queue`. This call is blocking, meaning it will wait until a message is available or the `timeout` is reached.

## 4. How They Work Together: A Unified Flow

1. **Initialization**: Your application creates an instance of `ThreadSafeRabbitMQWrapper`, providing all necessary RabbitMQ connection and setup details.

2. **Starting the Wrapper**: You call `wrapper.start()`. This launches a new background thread.

3. **Asyncio Setup**: Inside the new thread, an `asyncio` event loop starts, and your `RabbitMQClient` instance is created and connected to RabbitMQ.

4. **Publishing Path**:

   * Your main application thread calls `wrapper.publish_message("Hello", "my.key")`.

   * This message is placed into `wrapper.publish_queue`.

   * The background asyncio thread's loop picks up the message from `wrapper.publish_queue`.

   * The background thread then safely calls `await _client.publish("Hello", "my.key")`, sending the message to RabbitMQ.

5. **Consuming Path (if configured)**:

   * The `RabbitMQClient` (still in the background asyncio thread) starts consuming messages from its designated queue.

   * When a message arrives, the `_client`'s internal consumer callback (configured by the `ThreadSafeRabbitMQWrapper`) processes it.

   * The processed message data is then placed into `wrapper.incoming_messages_queue`.

   * Your main application thread calls `wrapper.get_consumed_message()`.

   * The message data is retrieved from `wrapper.incoming_messages_queue`, and your main thread can now work with it.

6. **Shutdown**: You call `wrapper.stop()`. This signals the background thread to shut down, which in turn gracefully closes the `RabbitMQClient`'s connection and stops its loops.

## 5. Usage Examples

Assume your classes are saved in a file named `rabbit_client.py`.

### 5.1 Publisher-Only Scenario

If you only need to send messages:

> 📝 **Note**: If you're publishing messages for Celery workers, make sure to use `publish_celery_task(...)` and configure the correct exchange and routing key matching your Celery app.

```python
import time
from aio_pika import ExchangeType
from rabbit_client import ThreadSafeRabbitMQWrapper # Assuming your code is in rabbit_client.py

def main_publisher():
    # Initialize wrapper for publishing only
    # No consumer_queue_name, etc., specified
    rabbitmq_publisher = ThreadSafeRabbitMQWrapper(
        amqp_url="amqp://guest:guest@localhost:5672/",
        publisher_exchange_name="my_application_events",
        publisher_exchange_type=ExchangeType.TOPIC,
        publisher_confirm=True,
        reconnect_interval=5,
        max_reconnect_attempts=3
    )

    try:
        print("Starting RabbitMQ publisher wrapper...")
        rabbitmq_publisher.start()
        print("Wrapper started. Publishing messages...")

        for i in range(5):
            message_body = f"Event_Message_{i}"
            routing_key = f"events.type.{i % 2}" # Alternate routing keys
            rabbitmq_publisher.publish_message(message_body, routing_key)
            print(f"Queued message '{message_body}' with routing key '{routing_key}'")
            time.sleep(1) # Simulate work

    except KeyboardInterrupt:
        print("Publisher interrupted. Shutting down...")
    finally:
        rabbitmq_publisher.stop()
        print("Publisher wrapper stopped.")

if __name__ == "__main__":
    main_publisher()
```

### 5.2 Consumer-Only Scenario

If you only need to consume messages:

```python
import time
from rabbit_client import ThreadSafeRabbitMQWrapper # Assuming your code is in rabbit_client.py

def main_consumer():
    # Initialize wrapper for consuming only
    rabbitmq_consumer = ThreadSafeRabbitMQWrapper(
        amqp_url="amqp://guest:guest@localhost:5672/",
        consumer_queue_name="my_consumer_queue",
        consumer_exchange_name="my_application_events", # Assume messages are routed here
        consumer_routing_key="events.#", # Listen to all events from this exchange
        use_dlx_for_consume=True
    )

    try:
        print("Starting RabbitMQ consumer wrapper...")
        rabbitmq_consumer.start()
        print(f"Wrapper started. Waiting for messages on queue 'my_consumer_queue'...")

        while True:
            # Poll for consumed messages. Blocking with a timeout.
            message = rabbitmq_consumer.get_consumed_message(timeout=1.0)
            if message:
                body = message.get("body")
                routing_key = message.get("routing_key")
                print(f"Received message: '{body}' (Routing Key: '{routing_key}')")
            else:
                print("No new messages...")
            time.sleep(0.5)

    except KeyboardInterrupt:
        print("Consumer interrupted. Shutting down...")
    finally:
        rabbitmq_consumer.stop()
        print("Consumer wrapper stopped.")

if __name__ == "__main__":
    main_consumer()
```

### 5.3 Combined Publisher and Consumer Scenario


```python
import time
from aio_pika import ExchangeType
from rabbit_client import ThreadSafeRabbitMQWrapper # Assuming your code is in rabbit_client.py

def main_combined():
    # Initialize wrapper for both publishing and consuming
    rabbitmq_client = ThreadSafeRabbitMQWrapper(
        amqp_url="amqp://guest:guest@localhost:5672/",
        publisher_exchange_name="order_processing_exchange",
        publisher_exchange_type=ExchangeType.TOPIC,
        publisher_confirm=True,
        consumer_queue_name="order_updates_queue",
        consumer_exchange_name="order_notifications_exchange",
        consumer_routing_key="order.status.#", # Listen for all order status updates
        use_dlx_for_consume=True,
        reconnect_interval=5
    )

    try:
        print("Starting combined RabbitMQ wrapper...")
        rabbitmq_client.start()
        print("Wrapper started. Publishing and consuming messages...")

        publish_counter = 0
        while True:
            # --- Publish ---
            message_body = f"Order_{publish_counter}_Processed"
            routing_key = f"order.processing.step.{publish_counter % 3}"
            rabbitmq_client.publish_message(message_body, routing_key)
            print(f"[PUBLISH] Queued: '{message_body}' to '{routing_key}'")
            publish_counter += 1

            # --- Consume ---
            message = rabbitmq_client.get_consumed_message(timeout=0.5) # Shorter timeout for responsiveness
            if message:
                body = message.get("body")
                routing_key = message.get("routing_key")
                print(f"[CONSUME] Received: '{body}' (Routing Key: '{routing_key}')")
            else:
                pass # print("No new consumed messages...")

            time.sleep(1) # Adjust sleep based on desired publishing/consuming rate

    except KeyboardInterrupt:
        print("Application interrupted. Shutting down...")
    finally:
        rabbitmq_client.stop()
        print("Combined wrapper stopped.")

if __name__ == "__main__":
    # You would typically run one of these main functions based on your application's role
    # For demonstration, you can uncomment one to test.
    # main_publisher()
    # main_consumer()
    main_combined()

```

## 6. Benefits of this Architecture

* **Thread Safety:** Safely interact with RabbitMQ from any Python thread.

* **Reliability:** Automatic reconnection, channel recovery, and DLX mechanisms ensure high availability and message integrity.

* **Asynchronous Efficiency:** The underlying `aio-pika` client provides non-blocking, efficient I/O for high-throughput scenarios.

* **Separation of Concerns:** The `RabbitMQClient` focuses on asynchronous RabbitMQ specifics, while the `ThreadSafeRabbitMQWrapper` handles the complexities of bridging to synchronous, multi-threaded environments.

* **Flexibility:** A single wrapper instance can perform both publishing and consuming roles, or be specialized for one, depending on the configuration parameters provided.

* **Simplified Application Code:** Your main application logic doesn't need to directly manage `asyncio` event loops or complex `aio-pika` objects, leading to cleaner code.


This architecture provides a robust and adaptable foundation for integrating RabbitMQ into various Python applications, especially those where thread-safety and reliable message handling are paramount.
