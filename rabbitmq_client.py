import asyncio
import json
import logging
from functools import wraps
import time
from typing import Callable, Any, Optional, Dict
import threading
import queue
import uuid
import aio_pika
from aio_pika.abc import AbstractIncomingMessage, AbstractChannel, AbstractConnection, AbstractQueue, AbstractExchange
from aio_pika import ExchangeType, DeliveryMode, exceptions
from aio_pika import Message
import logging
from logging.handlers import RotatingFileHandler

log_formatter = logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)

# File handler with rotation (keeps 5 backup logs, each max 10MB)
log_file = 'rabbit_client.log'
file_handler = RotatingFileHandler(
    log_file, maxBytes=10*1024*1024, backupCount=5, encoding='utf-8'
)
file_handler.setFormatter(log_formatter)
logger.addHandler(file_handler)

# --- Decorators for Connection Handling ---

def requires_connection(func: Callable[..., Any]) -> Callable[..., Any]:
    """
    Decorator to ensure a connection is established before executing a method.
    Attempts to reconnect if the connection is not active.
    """
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        if not self.is_connected:
            logger.warning(f"Connection not active for {func.__name__}. Attempting to reconnect...")
            await self.connect()
            if not self.is_connected:
                logger.error(f"Failed to establish connection for {func.__name__}. Aborting.")
                raise ConnectionError("RabbitMQ connection not available.")
        return await func(self, *args, **kwargs)
    return wrapper

def requires_channel(func: Callable[..., Any]) -> Callable[..., Any]:
    """
    Decorator to ensure a channel is established before executing a method.
    Attempts to reconnect if the channel is not active.
    """
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        if not self.channel or self.channel.is_closed:
            logger.warning(f"Channel not active for {func.__name__}. Attempting to create channel...")
            await self._create_channel()
            if not self.channel or self.channel.is_closed:
                logger.error(f"Failed to create channel for {func.__name__}. Aborting.")
                raise ConnectionError("RabbitMQ channel not available.")
        return await func(self, *args, **kwargs)
    return wrapper

# --- Main RabbitMQ Client Class ---

class RabbitMQClient:
    """
    A robust, asynchronous, and flexible RabbitMQ client for both publishing and consuming messages.

    Features:
    - Asynchronous implementation using aio-pika and asyncio.
    - Automatic reconnection and channel recovery for resilient operations.
    - Unified interface for both publisher and consumer roles.
    - Configurable exchanges, queues, bindings, and prefetch settings.
    - Built-in Dead Letter Exchange (DLX) and retry queue support for fault-tolerant message processing.
    - Retry logic with TTL and redirect to DLQ after maximum attempts.
    - Publisher confirms for guaranteed delivery acknowledgment from the broker.
    - Comprehensive error handling and detailed structured logging.
    - Production-ready design, suitable for microservices, task queues, or event-driven systems.

    DLX Support (Optional):
    - If `use_dlx_for_consume=True`, the client creates a retry queue and a dead-letter queue for the consumer.
    - Failed messages are re-published to the retry queue with a TTL and eventually re-delivered to the original queue.
    - After exceeding the max retry attempts, messages are routed to a dead-letter queue (DLQ).
    """


    def __init__(self,
                 amqp_url: str,
                 reconnect_interval: int = 2,
                 max_reconnect_attempts: int = 0, # 0 means infinite retries
                 prefetch_count: int = 10,
                 consumer_queue_name: Optional[str] = None,
                 consumer_exchange_name: Optional[str] = None,
                 consumer_routing_key: Optional[str] = None,
                 consumer_queue_args: Optional[Dict[str, Any]] = None,
                 celery_publish_exchange: Optional[str] = None,
                 celery_publish_routing_key: Optional[str] = None,
                 publisher_exchange_name: str = 'amq.topic', # Default to topic exchange for pub/sub
                 publisher_exchange_type: ExchangeType = ExchangeType.TOPIC,
                 publisher_exchange_durable: bool = True,
                 publisher_confirm: bool = True,
                 use_dlx_for_consume=True):
        """
        Initializes the RabbitMQ client.

        Args:
            amqp_url (str): The RabbitMQ connection URL (e.g., "amqp://guest:guest@localhost:5672/").
            reconnect_interval (int): Time in seconds to wait before attempting a reconnection.
            max_reconnect_attempts (int): Maximum number of reconnection attempts. 0 for infinite.
            prefetch_count (int): Maximum number of unacknowledged messages the consumer will receive.
            consumer_queue_name (Optional[str]): Name of the queue to consume from. If None, a unique
                                                  exclusive queue will be created (suitable for pub/sub).
            consumer_exchange_name (Optional[str]): Name of the exchange for the consumer to bind to.
                                                     Required if consumer_queue_name is provided.
            consumer_routing_key (Optional[str]): Routing key for the consumer's queue binding.
            consumer_queue_args (Optional[Dict[str, Any]]): Additional arguments for queue declaration (e.g., {'x-max-length': 1000}).
            publisher_exchange_name (str): Name of the exchange for publishing messages.
            publisher_exchange_type (ExchangeType): Type of the publisher exchange (e.g., ExchangeType.TOPIC, FANOUT, DIRECT).
            celery_publish_exchange (Optional[str]): The name of the exchange to which Celery task messages will be published.
            celery_publish_routing_key (Optional[str]): The default routing key for publishing Celery task messages.
            publisher_exchange_durable (bool): Whether the publisher exchange should survive a RabbitMQ restart.
            publisher_confirm (bool): Enable publisher confirms for reliable message publishing.
        """
        self.amqp_url: str = amqp_url
        self.reconnect_interval: int = reconnect_interval
        self.max_reconnect_attempts: int = max_reconnect_attempts
        self.prefetch_count: int = prefetch_count

        # Consumer specific configurations
        self.consumer_queue_name: Optional[str] = consumer_queue_name
        self.consumer_exchange_name: Optional[str] = consumer_exchange_name
        self.consumer_routing_key: Optional[str] = consumer_routing_key
        self.consumer_queue_args: Optional[Dict[str, Any]] = consumer_queue_args or {}
        self.on_message_callback: Optional[Callable[[AbstractIncomingMessage], Any]] = None
        self.consumer_task: Optional[asyncio.Task] = None

        
        # DLQ and Retry queue name
        self.use_dlx_for_consume=use_dlx_for_consume
        self.max_retries = 5
        self.retry_delay_ms = 5000  # 5 seconds retry delay

        self.retry_queue_name = f"{self.consumer_queue_name}.retry"
        self.dlq_queue_name = f"{self.consumer_queue_name}.dlq"


        # Publisher specific configurations
        self.publisher_exchange_name: str = publisher_exchange_name
        self.publisher_exchange_type: ExchangeType = publisher_exchange_type
        self.publisher_exchange_durable: bool = publisher_exchange_durable
        self.publisher_confirm: bool = publisher_confirm
        self.celery_publish_exchange=celery_publish_exchange
        self.celery_publish_routing_key=celery_publish_routing_key

        # Internal state
        self._connection: Optional[AbstractConnection] = None
        self._channel: Optional[AbstractChannel] = None
        self._consumer_queue: Optional[AbstractQueue] = None
        self._publisher_exchange: Optional[AbstractExchange] = None
        self._celery_publisher_exchange: Optional[AbstractExchange] = None
        self._reconnect_attempts: int = 0
        self._closing: bool = False

    @property
    def is_connected(self) -> bool:
        """Checks if the connection is active."""
        return self._connection is not None and not self._connection.is_closed

    @property
    def channel(self) -> Optional[AbstractChannel]:
        """Returns the current channel."""
        return self._channel

    async def connect(self) -> None:
        """Establishes a connection to RabbitMQ with exponential backoff and retries."""
        if self.is_connected:
            logger.info("Already connected to RabbitMQ.")
            return

        self._closing = False # Reset closing flag on new connection attempt

        while not self._closing:
            try:
                logger.info(f"Attempting to connect to RabbitMQ at {self.amqp_url} "
                            f"(attempt {self._reconnect_attempts + 1}/{self.max_reconnect_attempts if self.max_reconnect_attempts > 0 else 'infinity'})...")
                self._connection = await aio_pika.connect_robust(
                    self.amqp_url,
                    client_properties={"connection_name": "unified_rabbitmq_client"},
                    loop=asyncio.get_event_loop() # Explicitly pass loop for robustness
                )
                self._connection.close_callbacks(self._on_connection_close)
              
                
                self._reconnect_attempts = 0
                logger.info("Successfully connected to RabbitMQ.")

                # Create channel and declare exchange/queue immediately after connection
                await self._create_channel()
                await self._declare_publisher_exchange()
                await self._declare_celery_publisher_exchange()

                return

            except exceptions.AMQPConnectionError as e:
                self._reconnect_attempts += 1
                logger.error(f"Failed to connect to RabbitMQ: {e}")
                if self.max_reconnect_attempts > 0 and \
                   self._reconnect_attempts >= self.max_reconnect_attempts:
                    logger.critical(f"Exceeded max reconnect attempts ({self.max_reconnect_attempts}). Giving up.")
                    self._closing = True # Stop trying to reconnect if max attempts reached
                    raise ConnectionError("Maximum RabbitMQ reconnection attempts reached.")

                # Implement exponential backoff (with a cap)
                wait_time = min(self.reconnect_interval * (2 ** (self._reconnect_attempts - 1)), 60) # Cap at 60 seconds
                logger.info(f"Retrying connection in {wait_time} seconds...")
                await asyncio.sleep(wait_time)
            except Exception as e:
                logger.error(f"An unexpected error occurred during connection: {e}")
                self._reconnect_attempts += 1
                # Still apply backoff for unexpected errors
                wait_time = min(self.reconnect_interval * (2 ** (self._reconnect_attempts - 1)), 60)
                logger.info(f"Retrying connection in {wait_time} seconds...")
                await asyncio.sleep(wait_time)


    async def _on_connection_close(self, sender: Any, exc: Optional[Exception]) -> None:
        """Callback for when the connection closes."""
        if self._closing:
            logger.info("Connection closing initiated by client. Not attempting reconnect.")
            return

        logger.warning(f"RabbitMQ connection closed. Reason: {exc}. Attempting to reconnect...")
        self._connection = None
        self._channel = None
        self._consumer_queue = None
        self._publisher_exchange = None
        self._celery_publisher_exchange = None
        asyncio.create_task(self.connect()) # Start reconnection in background

    async def _create_channel(self) -> None:
        """Creates a new channel and sets prefetch count."""
        if self._connection is None or self._connection.is_closed:
            logger.warning("No active connection to create a channel.")
            return

        try:
            # Pass publisher_confirms to channel creation
            self._channel = await self._connection.channel(publisher_confirms=self.publisher_confirm)
            await self._channel.set_qos(prefetch_count=self.prefetch_count)
            logger.info(f"Channel created with prefetch_count={self.prefetch_count}, "
                        f"publisher_confirm={self.publisher_confirm}.")
        # try:
        #     self._channel = await self._connection.channel()
        #     if self.publisher_confirm:
        #         await self._channel.confirm_delivery()
        #     await self._channel.set_qos(prefetch_count=self.prefetch_count)
        #     logger.info(f"Channel created with prefetch_count={self.prefetch_count}.")
        except Exception as e:
            logger.error(f"Failed to create channel: {e}")
            self._channel = None # Ensure channel is None if creation fails
            raise # Re-raise to be caught by connection logic if necessary

    async def _declare_publisher_exchange(self) -> None:
        """Declares the publisher exchange."""
        if self.channel is None or self.channel.is_closed:
            logger.warning("No active channel to declare publisher exchange.")
            return

        try:
            self._publisher_exchange = await self.channel.declare_exchange(
                self.publisher_exchange_name,
                self.publisher_exchange_type,
                durable=self.publisher_exchange_durable
            )
            logger.info(f"Publisher exchange '{self.publisher_exchange_name}' declared as type "
                        f"'{self.publisher_exchange_type.value}', durable={self.publisher_exchange_durable}.")
        except Exception as e:
            logger.error(f"Failed to declare publisher exchange '{self.publisher_exchange_name}': {e}")
            self._publisher_exchange = None # Ensure exchange is None if declaration fails
            raise

    async def _declare_celery_publisher_exchange(self) -> None:
        """
        Declares the Celery exchange for publishing tasks.

        Exchange type should match what Celery is using (default: `ExchangeType.DIRECT`).
        The exchange must be durable and match the worker's expectation.

        Does nothing if `celery_publish_exchange` is not configured.

        Raises:
            Exception: If the exchange declaration fails.
        """
        if not self.celery_publish_exchange:
            return
        
        if self.channel is None or self.channel.is_closed:
            logger.warning("No active channel to declare celery publisher exchange.")
            return

        try:
            self._celery_publisher_exchange = await self.channel.declare_exchange(
                self.celery_publish_exchange,
                ExchangeType.DIRECT,
                durable=True
            )
            logger.info(f"Celery Publisher exchange '{self.celery_publish_exchange}' declared as type "
                        f"'DIRECT', durable=True.")
        except Exception as e:
            logger.error(f"Failed to declare celery publisher exchange '{self.celery_publish_exchange}': {e}")
            self._celery_publisher_exchange = None # Ensure exchange is None if declaration fails
            raise

    @requires_channel
    async def publish(self,
                      message: bytes,
                      routing_key: str,
                      exchange_name: Optional[str] = None,
                      mandatory: bool = False,
                      immediate: bool = False,
                      timeout: Optional[float] = None) -> None:
        """
        Publishes a message to a specified exchange.

        Args:
            message (bytes): The message body to publish.
            routing_key (str): The routing key for the message.
            exchange_name (Optional[str]): The name of the exchange to publish to.
                                           Defaults to the client's publisher_exchange_name.
            properties (Optional[aio_pika.MessageProperties]): Message properties.
                                                                Defaults to persistent delivery.
            mandatory (bool): If true, returns the message to the publisher if it cannot be routed.
            immediate (bool): If true, returns the message to the publisher if it cannot be delivered
                              to a consumer immediately.
            timeout (Optional[float]): Timeout for publisher confirm.
        """
        exchange_to_use = exchange_name if exchange_name is not None else self.publisher_exchange_name

        if self._publisher_exchange is None or self._publisher_exchange.name != exchange_to_use:
            # If the exchange needs to be different or not yet declared, declare it.
            # This allows flexible publishing to various exchanges from one client.
            try:
                # Create a temporary channel for other exchanges if needed, or re-declare primary
                if self.channel is None:
                    await self._create_channel() # Recreate if lost
                await self._declare_publisher_exchange()
                
                logger.debug(f"Dynamically declared exchange '{exchange_to_use}' for publishing.")
            except Exception as e:
                logger.error(f"Failed to declare exchange '{exchange_to_use}' for publishing: {e}")
                raise

        # Default properties to persistent delivery
        # if properties is None:
        #     properties = aio_pika.MessageProperties(delivery_mode=DeliveryMode.PERSISTENT)

        try:
            await self._publisher_exchange.publish(
                aio_pika.Message(message),
                routing_key=routing_key,
                mandatory=mandatory,
                immediate=immediate,
                timeout=timeout
            )
            logger.info(f"Published message to exchange '{exchange_to_use}' with routing key '{routing_key}'.")
        except exceptions.ChannelClosed as e:
            logger.error(f"Channel closed while publishing. Message not sent: {e}")
            # Reconnection logic will handle channel recovery, but message is lost unless retried by caller
            raise
        except exceptions.ConnectionClosed as e:
            logger.error(f"Connection closed while publishing. Message not sent: {e}")
            # Reconnection logic will handle connection recovery, but message is lost unless retried by caller
            raise
        except Exception as e:
            logger.error(f"Failed to publish message: {e}")
            raise
    

    def _build_celery_task_message(self, task_name: str, args=None, kwargs=None) -> Message:
        """
        Constructs a Celery-compatible `aio_pika.Message` object.

        This method populates the message body with JSON-encoded arguments and
        sets the necessary headers (e.g., 'task', 'id', 'lang') that Celery
        workers expect to find.
        Headers include:
        - `task`: Full task name
        - `id`: Unique UUID for the task
        - `argsrepr`, `kwargsrepr`: Stringified args/kwargs
        - `origin`: Sender identifier ("engine")

        

        Args:
            task_name (str): The full dotted path to the Celery task function (e.g., 'tasks.add').
            args (list): A list of positional arguments for the task.
            kwargs (dict): A dictionary of keyword arguments for the task.

        Returns:
            aio_pika.Message: Ready-to-publish message with proper headers and body.
        """
        task_id = str(uuid.uuid4())
        timestamp = int(time.time())
        args = args or []
        kwargs = kwargs or {}

        body = json.dumps([args, kwargs, None])
        body_bytes = body.encode("utf-8")

        headers = {
            "lang": "py",
            "task": task_name,
            "id": task_id,
            "argsrepr": repr(args),
            "kwargsrepr": repr(kwargs),
            "origin": "engine",
        }

        return Message(
            body_bytes,
            content_type="application/json",
            content_encoding="utf-8",
            headers=headers,
            correlation_id=task_id,
            delivery_mode=DeliveryMode.PERSISTENT,
            priority=0,
            timestamp=timestamp,
        )
    
    @requires_channel
    async def publish_celery_task(self,
                                  task_name: str,
                                  routing_key: Optional[str] = None,
                                  args=None,
                                  kwargs=None,
                                  mandatory: bool = False,
                                  immediate: bool = False,
                                  timeout: Optional[float] = None) -> None:
        """
        Publishes a Celery-compatible task message to RabbitMQ.

        This simulates Celery's internal AMQP protocol so that Celery workers can
        consume the message as if it was created via `task.delay(...)`.

        Args:
            task_name (str): Full dotted path to the Celery task (e.g., 'myapp.tasks.do_work').
            routing_key (str): Target Celery queue, if not provided, uses the client's configured default.
            args (list): Positional arguments to the task.
            kwargs (dict): Keyword arguments to the task.
            mandatory (bool): If true, returns the message to the publisher if it cannot be routed.
            immediate (bool): If true, returns the message to the publisher if it cannot be delivered
                            to a consumer immediately.
            timeout (Optional[float]): Timeout for publisher confirm.
        Raises:
            Exception: If publishing or exchange declaration fails.
        """
        if self.celery_publish_exchange is None:
            logger.warning("Celery exchange not set, failed to publish message for celery")
            return
        
        publish_routing_key = routing_key if routing_key is not None else self.celery_publish_routing_key
        if publish_routing_key is None:
            logger.warning("Celery routing_key not set, failed to publish message for celery")
            return

        exchange_to_use = self.celery_publish_exchange
        if self._celery_publisher_exchange is None:
            try:
                if self.channel is None:
                    await self._create_channel() # Recreate if lost
                await self._declare_celery_publisher_exchange()
                
                logger.debug(f"Exchange '{exchange_to_use}' declared for celery message publishing.")
            except Exception as e:
                logger.error(f"Failed to declare exchange '{exchange_to_use}' for celery message publishing: {e}")
                raise

        message = self._build_celery_task_message(task_name, publish_routing_key, args, kwargs)

        try:
            await self._celery_publisher_exchange.publish(
                message,
                routing_key=publish_routing_key,
                mandatory=mandatory,
                immediate=immediate,
                timeout=timeout
            )
            logger.info(f"Published Celery task '{task_name}' with publish_routing_key '{publish_routing_key}' with args={args} kwargs={kwargs}")
        except exceptions.ChannelClosed as e:
            logger.error(f"Channel closed while publishing Celery task '{task_name}': {e}")
            raise
        except exceptions.ConnectionClosed as e:
            logger.error(f"Connection closed while publishing Celery task '{task_name}': {e}")
            raise
        except Exception as e:
            logger.exception(f"Unexpected error publishing Celery task '{task_name}': {e}")
            raise


    async def _setup_retry_dlx(self):
        """
        Sets up the Dead Letter Exchange (DLX), retry queue, and dead-letter queue (DLQ) for the consumer.

        This method declares:
        - A DLX exchange (fanout) for failed messages.
        - A DLQ queue bound to the DLX to store permanently failed messages.
        - A retry queue with TTL and dead-lettering back to the original queue.

        The retry mechanism works as follows:
        - Messages that fail are re-published to the retry queue.
        - After a configurable delay (`self.retry_delay_ms`), messages expire and are routed back to the main queue.
        - If retry attempts exceed `self.max_retries`, the message is sent to the DLQ via the DLX.

        Returns:
            dict: A dictionary containing `x-dead-letter-exchange` arguments to be used when declaring the main queue,
                enabling dead-letter routing to the DLX.
        Raises:
            Exception: If any of the exchange or queue declarations fail.
        """

        if self.channel is None or self.channel.is_closed:
            logger.warning("No active channel to declare publisher exchange.")
            return
        

        dlq_queue = await self.channel.declare_queue(self.dlq_queue_name, durable=True)

        # Retry Queue (with TTL + DLX to original queue)
        logger.info(f"Creating retry queue {self.retry_queue_name}")
        await self.channel.declare_queue(
            self.retry_queue_name,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",  # Use default exchange
                "x-dead-letter-routing-key": self.consumer_queue_name,
                "x-message-ttl": self.retry_delay_ms
            }
        )
        logger.info(f"DLX setup completed")
      
    @requires_channel
    async def start_consuming(self, message_callback: Callable[[AbstractIncomingMessage], Any]) -> None:
        """
        Starts consuming messages from the configured queue.

        If `use_dlx_for_consume` is enabled, this sets up the retry and DLQ infrastructure before consuming.

        Args:
            message_callback (Callable[[AbstractIncomingMessage], Any]): 
                An async function to handle each incoming message. 
                Do NOT manually ack/nack within this callback, as the client handles it using `message.process()`.

        Behavior:
        - Declares the main consumer queue (durable by default).
        - Binds the queue to the provided exchange and routing key.
        - Starts a background task to consume messages via an internal loop.
        - Automatically stops and restarts the consumer if already running.

        Raises:
            Exception: If queue or exchange declaration fails.
        """


        if self.use_dlx_for_consume:
            await self._setup_retry_dlx()
        #     self.consumer_queue_args.update(await self._setup_retry_dlx())
        #     logger.info(f"consumer_queue_args: {self.consumer_queue_args}")


        if self._consumer_queue:
            logger.warning("Consumer is already running. Stopping existing consumer before starting a new one.")
            await self.stop_consuming()

        self.on_message_callback = message_callback

        try:
            # Declare the consumer queue
            if self.consumer_queue_name:
                self._consumer_queue = await self.channel.declare_queue(
                    self.consumer_queue_name,
                    durable=True, # Default to durable queues for consumers
                    arguments=self.consumer_queue_args
                )
                logger.info(f"Consumer queue '{self.consumer_queue_name}' declared.")

            else:
                # Declare a unique, exclusive, auto-delete queue for pub/sub (temporary consumer)
                self._consumer_queue = await self.channel.declare_queue(
                    name="", # Let RabbitMQ generate a unique name
                    exclusive=True,
                    auto_delete=True,
                    arguments=self.consumer_queue_args
                )
                logger.info(f"Consumer queue '{self._consumer_queue.name}' (exclusive, auto-delete) declared.")
            

            # Bind the consumer queue to the exchange if specified
            if self.consumer_exchange_name:
                consumer_exchange = await self.channel.declare_exchange(
                    self.consumer_exchange_name,
                    ExchangeType.TOPIC, # Default to topic for binding, consider making configurable
                    durable=True # Default to durable
                )
                await self._consumer_queue.bind(
                    consumer_exchange,
                    routing_key=self.consumer_routing_key or "#" # Default to all if not specified
                )
                logger.info(f"Consumer queue '{self._consumer_queue.name}' bound to exchange "
                            f"'{self.consumer_exchange_name}' with routing key '{self.consumer_routing_key or '#'}'.")

            # Start consuming
            self.consumer_task = asyncio.create_task(self._start_consuming_loop())
            logger.info(f"Started consuming on queue '{self._consumer_queue.name}'.")

        except Exception as e:
            logger.error(f"Failed to start consuming: {e}")
            self._consumer_queue = None
            if self.consumer_task:
                self.consumer_task.cancel()
            raise

    async def _start_consuming_loop(self) -> None:
        """
        Internal method that runs the consumer message loop.

        For each message:
        - Wraps it in an `async with message.process(ignore_processed=True)` context for automatic acknowledgement.
        - Calls the user-defined `on_message_callback(message)` to process it.
        - On failure:
            - If DLX is enabled (`use_dlx_for_consume`), the message is re-published to a retry queue.
            - Retry attempts are tracked via the `x-retries` header.
            - After `max_retries`, the message is routed to a DLQ.
            - No manual `ack()`/`nack()` calls are needed — they are handled by the context manager.

        Handles connection/channel closures and triggers auto-reconnect logic when needed.

        Notes:
            - This method should not be called directly; use `start_consuming()` instead.
            - The loop auto-recovers from common RabbitMQ connection errors.
    """
        if not self._consumer_queue:
            logger.error("Consumer queue not initialized. Cannot start consuming loop.")
            return

        try:
            logger.info(f"_start_consuming_loop {self._consumer_queue}")
            async with self._consumer_queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process(ignore_processed=True):
                        try:
                            await self.on_message_callback(message)
                            # Messages are automatically acknowledged if no exception is raised
                            # within the 'async with message.process():' block.
                            # For explicit acknowledgment, use: await message.ack()
                        except Exception as e:
                            logger.error(f"Error processing message: {e}. Message body: {message.body.decode()}")
                            if self.use_dlx_for_consume:
                                try:
                                    headers = message.headers or {}
                                    retries = headers.get("x-retries", 0) + 1
                                    if retries > self.max_retries:
                                        logger.warning(f"Exceeded retry attempts for message. Moving to DLQ. Body: {message.body.decode()}")
                                        await self.channel.default_exchange.publish(
                                            Message(
                                                message.body,
                                                delivery_mode=DeliveryMode.PERSISTENT,
                                                headers=headers,
                                            ),
                                            routing_key=self.dlq_queue_name,
                                        )
                                    else:
                                        logger.info(f"Retrying message (attempt {retries})...")
                                        headers["x-retries"] = retries
                                        await self.channel.default_exchange.publish(
                                            Message(
                                                message.body,
                                                delivery_mode=DeliveryMode.PERSISTENT,
                                                headers=headers,
                                            ),
                                            routing_key=self.retry_queue_name,
                                        )
                                    # await message.ack()  # Always ack after redirection
                                except Exception as retry_err:
                                    logger.critical(f"Failed to requeue or DLQ message: {retry_err}")
                                    # await message.reject(requeue=False)  # Give up
                            # await message.nack(requeue=True) # Nack and requeue if processing fails
        except asyncio.CancelledError as e:
            logger.info("Consumer loop cancelled.")
        except exceptions.ChannelClosed as e:
            logger.warning(f"Consumer channel closed: {e}. Attempting to reconnect and restart consuming.")
            # Connection close callback will handle reconnection and re-initiate consuming
        except exceptions.ConnectionClosed as e:
            logger.warning(f"Consumer connection closed: {e}. Attempting to reconnect and restart consuming.")
            # Connection close callback will handle reconnection and re-initiate consuming
        except Exception as e:
            logger.critical(f"Unhandled exception in consumer loop: {e}. Restarting consumer...")
            # This might happen due to various network issues not immediately caught by aio-pika's robust connection.
            # Re-establishing connection and consumer is the safest bet.
            self._connection = None
            self._channel = None
            self._consumer_queue = None
            await self.connect()
            if self.on_message_callback: # Only restart if a callback was set
                await self.start_consuming(self.on_message_callback)


    async def stop_consuming(self) -> None:
        """Stops the consumer gracefully."""
        if self.consumer_task:
            self.consumer_task.cancel()
            try:
                await self.consumer_task
            except asyncio.CancelledError:
                logger.info("Consumer task successfully cancelled.")
            self.consumer_task = None
        else:
            logger.info("No active consumer task to stop.")

    async def close(self) -> None:
        """Closes the RabbitMQ connection and stops all operations."""
        self._closing = True
        logger.info("Closing RabbitMQ client connections...")
        await self.stop_consuming()
        if self._channel and not self._channel.is_closed:
            await self._channel.close()
            logger.info("Channel closed.")
        if self._connection and not self._connection.is_closed:
            await self._connection.close()
            logger.info("Connection closed.")
        self._connection = None
        self._channel = None
        self._consumer_queue = None
        self._publisher_exchange = None
        logger.info("RabbitMQ client gracefully shut down.")


class ThreadSafeRabbitMQWrapper:
    def __init__(self, 
             amqp_url: str,
             consumer_queue_name: Optional[str] = None,
             consumer_exchange_name: Optional[str] = None,
             consumer_routing_key: Optional[str] = None,
             consumer_queue_args: Optional[Dict[str, Any]] = None,
             publisher_exchange_name: str = "amq.topic",
             publisher_exchange_type: ExchangeType = ExchangeType.TOPIC,
             publisher_exchange_durable: bool = True,
             publisher_confirm: bool = True,
             celery_publish_exchange: Optional[str] = None,
             celery_publish_routing_key: Optional[str] = None,
             use_dlx_for_consume: bool = True,
             reconnect_interval: int = 2,
             max_reconnect_attempts: int = 0,
             prefetch_count: int = 10):
        """
        Initializes the ThreadSafeRabbitMQWrapper.

        This wrapper provides a thread-safe interface to the asynchronous RabbitMQClient,
        allowing interaction with RabbitMQ from synchronous code or multiple threads
        without directly managing the asyncio event loop.

        Args:
            amqp_url (str): The RabbitMQ connection URL.
            consumer_queue_name (Optional[str]): Name of the queue for the internal consumer to listen on.
            consumer_exchange_name (Optional[str]): Name of the exchange to bind the consumer queue to.
            consumer_routing_key (Optional[str]): Routing key for the consumer queue binding.
            consumer_queue_args (Optional[Dict[str, Any]]): Additional arguments for consumer queue declaration.
            publisher_exchange_name (str): Name of the default exchange for publishing messages.
            publisher_exchange_type (ExchangeType): Type of the publisher exchange.
            publisher_exchange_durable (bool): Whether the publisher exchange is durable.
            publisher_confirm (bool): Enable publisher confirms.
            celery_publish_exchange (Optional[str]): The name of the exchange for Celery task publishing.
            celery_publish_routing_key (Optional[str]): The default routing key for Celery tasks.
            use_dlx_for_consume (bool): If True, enables Dead Letter Exchange for consumed messages.
            reconnect_interval (int): Time in seconds to wait before attempting a reconnection.
            max_reconnect_attempts (int): Maximum number of reconnection attempts (0 for infinite).
            prefetch_count (int): Maximum number of unacknowledged messages the consumer will receive.
        """
        self.amqp_url: str = amqp_url
        self.reconnect_interval: int = reconnect_interval
        self.max_reconnect_attempts: int = max_reconnect_attempts
        self.prefetch_count: int = prefetch_count

        # Consumer specific configurations
        self.consumer_queue_name: Optional[str] = consumer_queue_name
        self.consumer_exchange_name: Optional[str] = consumer_exchange_name
        self.consumer_routing_key: Optional[str] = consumer_routing_key
        self.consumer_queue_args: Optional[Dict[str, Any]] = consumer_queue_args or {}

        self.on_message_callback: Optional[Callable[[AbstractIncomingMessage], Any]] = None
        self.consumer_task: Optional[asyncio.Task] = None

        self.publisher_exchange_name: str = publisher_exchange_name
        self.publisher_exchange_type: ExchangeType = publisher_exchange_type
        self.publisher_exchange_durable: bool = publisher_exchange_durable
        self.publisher_confirm: bool = publisher_confirm

        self.celery_publish_exchange=celery_publish_exchange
        self.celery_publish_routing_key=celery_publish_routing_key

        self.use_dlx_for_consume = use_dlx_for_consume

        # Queues and threading
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._client: Optional[RabbitMQClient] = None

        # Thread-safe queues for communication
        self.publish_queue = queue.Queue() # For sending messages to the asyncio thread for publishing
        self.incoming_messages_queue = queue.Queue() # For receiving consumed messages from the asyncio thread

        self._stop_event = threading.Event()

    def is_connected(self) -> bool:
        """Checks if the underlying RabbitMQClient is connected."""
        if self._client:
            return self._client.is_connected
        return False
    
    def _run_asyncio_loop(self):
        """
        Internal method that runs the asyncio event loop in a dedicated thread.
        This thread manages the asynchronous RabbitMQClient, handling both
        publishing requests from other threads and pushing consumed messages
        to a thread-safe queue.
        """
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

        
        self._client = RabbitMQClient(
            amqp_url=self.amqp_url,
            consumer_queue_name=self.consumer_queue_name,
            consumer_exchange_name=self.consumer_exchange_name,
            consumer_routing_key=self.consumer_routing_key,
            publisher_exchange_name=self.publisher_exchange_name,
            publisher_exchange_type=self.publisher_exchange_type,
            publisher_exchange_durable=self.publisher_exchange_durable,
            publisher_confirm=self.publisher_confirm,
            celery_publish_exchange=self.celery_publish_exchange,
            celery_publish_routing_key=self.celery_publish_routing_key,
            use_dlx_for_consume=self.use_dlx_for_consume,
            reconnect_interval=self.reconnect_interval,
            max_reconnect_attempts=self.max_reconnect_attempts,
            prefetch_count=self.prefetch_count
        )

        async def _consumer_callback(message: AbstractIncomingMessage):
            async with message.process(requeue=True):
                # Put the message body (or processed data) into the thread-safe queue
                self.incoming_messages_queue.put({
                    "body": message.body.decode(),
                    "routing_key": message.routing_key
                })
                logger.debug(f"Consumer callback pushed message to queue: {message.body.decode()}")

        async def _asyncio_main():
            await self._client.connect()
            if self.consumer_queue_name: # Only start consuming if a queue name was provided
                await self._client.start_consuming(_consumer_callback)

            while not self._stop_event.is_set():
                try:
                    # Check for messages to publish from other threads
                    pub_data = self.publish_queue.get_nowait()
                    if pub_data.get("type") == "celery_task":
                        await self._client.publish_celery_task(
                            task_name=pub_data["task_name"],
                            args=pub_data["args"],
                            kwargs=pub_data["kwargs"],
                            routing_key=pub_data.get("routing_key")
                        )
                    else:
                        message_body = pub_data["message"].encode()
                        routing_key = pub_data["routing_key"]
                        await self._client.publish(message_body, routing_key)

                    logger.info(f"Published message from queue: {message_body.decode()}")
                except queue.Empty:
                    await asyncio.sleep(0.1) # Briefly yield to other tasks if no messages to publish
                except Exception as e:
                    logger.error(f"Error publishing message from queue: {e}")
                    logger.exception(e)
                    #TODO
                    # Decide on retry logic or put back to a dead-letter queue if needed
                    # self.publish_queue.put(pub_data) # Re-add to queue for retry
            logger.info("Asyncio loop stopping.")
            await self._client.close()

        self._loop.run_until_complete(_asyncio_main())
        self._loop.close()
        logger.info("Asyncio loop closed.")

    def start(self):
        """
        Starts the dedicated asyncio thread. This thread will establish
        and maintain the RabbitMQ connection, handling all asynchronous
        operations for publishing and consuming.
        """
        if self._thread is None or not self._thread.is_alive():
            self._thread = threading.Thread(target=self._run_asyncio_loop, daemon=True)
            self._thread.start()
            logger.info("RabbitMQ asyncio thread started.")

    def stop(self):
        """
        Signals the dedicated asyncio thread to stop and waits for its termination.
        This gracefully shuts down the RabbitMQ connection and all associated tasks.
        """
        if self._thread and self._thread.is_alive():
            self._stop_event.set()
            self._thread.join(timeout=10) # Wait for the thread to finish
            if self._thread.is_alive():
                logger.warning("RabbitMQ asyncio thread did not shut down gracefully.")
            logger.info("RabbitMQ asyncio thread stopped.")

    def publish_message(self, message: str, routing_key: str):
        """
        Puts a message into an internal thread-safe queue to be published
        by the dedicated asyncio thread. This method is non-blocking.

        Args:
            message (str): The message body to be published.
            routing_key (str): The routing key for the message.
        """
        self.publish_queue.put({"message": message, "routing_key": routing_key})
        logger.debug(f"Queued message for publishing: {message}")

    def publish_celery_task(self, task_name: str, routing_key=None, args=None, kwargs=None):
        """
        Thread-safe way to enqueue a Celery task from a synchronous context.

        This method places the task details into a thread-safe queue, which is then
        processed by the internal asyncio loop and published to RabbitMQ
        in a Celery-compatible format.

        Args:
            task_name (str): The full dotted path to the Celery task function.
            routing_key (Optional[str]): The routing key for the Celery task. If not provided,
                                        the default `celery_publish_routing_key` is used.
            args (list): Positional arguments for the task.
            kwargs (dict): Keyword arguments for the task.
        """
        payload = {
            "type": "celery_task",
            "task_name": task_name,
            "args": args or [],
            "kwargs": kwargs or {},
            "routing_key": routing_key
        }
        self.publish_queue.put(payload)


    def get_consumed_message(self, timeout: Optional[float] = None) -> Optional[Dict[str, Any]]:
        """
        Retrieves a consumed message from the internal thread-safe queue.
        This method is blocking and can be used by other threads to get messages.

        Args:
            timeout (Optional[float]): The maximum time (in seconds) to wait for a message.
                                        If None, waits indefinitely.

        Returns:
            Optional[Dict[str, Any]]: A dictionary containing the consumed message body
                                      and routing key, or None if the timeout occurs.
        """
        try:
            return self.incoming_messages_queue.get(timeout=timeout)
        except queue.Empty:
            return None
