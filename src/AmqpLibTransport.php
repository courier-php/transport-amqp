<?php
declare(strict_types = 1);

namespace Courier\Transports;

use Courier\Contracts\Messages\MessageInterface;
use Courier\Contracts\Serializers\SerializerInterface;
use Courier\Contracts\Transports\TransportInterface;
use Courier\Exceptions\TransportException;
use Courier\Serializers\JsonSerializer;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Channel\AMQPChannel;


class AmqpLibTransport implements TransportInterface {
  private SerializerInterface $serializer;
  /**
   * @var array<string, array<string, bool>>
   */
  private array $registeredRoutes = [];

  private AMQPStreamConnection $amqpConnection;
  private AMQPExchange $amqpExchange;
  /**
   * @var array<string, AMQPQueue>
   */
  private array $amqpQueues = [];
  private string $exchangeName = 'courier';

  private function getChannel(): AMQPChannel {
    static $channel = null;
    if ($channel === null) {
      if ($this->amqpConnection->isConnected() === false) {
        $this->amqpConnection->connect();
      }

      $channel = new AMQPChannel($this->amqpConnection);
    }

    return $channel;
  }

  /**
   * @return array<string, mixed>
   */
  private function messageAttributes(MessageInterface $message): array {
    $attributes = [];
    foreach ($message->getAttributes() as $key => $value) {
      switch ($key) {
        case 'appId':
          $attributes['app_id'] = $value;
          break;
        case 'correlationId':
          $attributes['correlation_id'] = $value;
          break;
        case 'deliveryMode':
          $attributes['delivery_mode'] = $value;
          break;
        case 'id':
          $attributes['message_id'] = $value;
          break;
        case 'replyTo':
          $attributes['reply_to'] = $value;
          break;
        case 'timestamp':
          $attributes['timestamp'] = strtotime($value);
          break;
        case 'userId':
          $attributes['user_id'] = $value;
          break;
        case 'expiration':
        case 'headers':
        case 'priority':
        case 'type':
          $attributes[$key] = $value;
          break;
        // default:
        //   if (isset($attributes['headers']) === false) {
        //     $attributes['headers'] = [];
        //   }

        //   $attributes['headers'][$key] = $value;
      }
    }

    return $attributes;
  }

  // private function fromEnvelope(AMQPEnvelope $envelope): MessageInterface {
  //   $message = $this->serializer->unserialize($envelope->getBody());
  //   $message->setAttributes(
  //     [
  //       'appId' => $envelope->getAppId(),
  //       'contentEncoding' => $envelope->getContentEncoding(),
  //       'contentType' => $envelope->getContentType(),
  //       'correlationId' => $envelope->getCorrelationId(),
  //       'deliveryMode' => $envelope->getDeliveryMode(),
  //       'expiration' => $envelope->getExpiration(),
  //       'headers' => $envelope->getHeaders(),
  //       'id' => $envelope->getMessageId(),
  //       'priority' => $envelope->getPriority(),
  //       'replyTo' => $envelope->getReplyTo(),
  //       'timestamp' => $envelope->getTimestamp(),
  //       'type' => $envelope->getType(),
  //       'userId' => $envelope->getUserId(),
  //     ]
  //   );

  //   return $message;
  // }

  private function setupServer(): void {
    static $ready = false;
    if ($ready === false) {
      // dead letter exchange, for rejected messages
      $amqpExchange = new AMQPExchange($this->getChannel());
      $amqpExchange->setName("{$this->exchangeName}.dead");
      $amqpExchange->setFlags(AMQP_DURABLE | AMQP_INTERNAL);
      $amqpExchange->setType(AMQP_EX_TYPE_FANOUT);
      $amqpExchange->declareExchange();

      // alternate exchange, for unrouted messages
      $amqpExchange = new AMQPExchange($this->getChannel());
      $amqpExchange->setName("{$this->exchangeName}.unrouted");
      $amqpExchange->setFlags(AMQP_DURABLE | AMQP_INTERNAL);
      $amqpExchange->setType(AMQP_EX_TYPE_FANOUT);
      $amqpExchange->declareExchange();

      // main exchange
      $this->amqpExchange = new AMQPExchange($this->getChannel());
      $this->amqpExchange->setName($this->exchangeName);
      $this->amqpExchange->setFlags(AMQP_DURABLE);
      $this->amqpExchange->setType(AMQP_EX_TYPE_DIRECT);
      $this->amqpExchange->setArgument('alternate-exchange', "{$this->exchangeName}.unrouted");
      $this->amqpExchange->declareExchange();

      // rejected messages queue
      $amqpQueue = new AMQPQueue($this->getChannel());
      $amqpQueue->setName("{$this->exchangeName}.dead");
      $amqpQueue->setFlags(AMQP_DURABLE);
      $amqpQueue->declareQueue();

      // unrouted messages queue (max 1000 messages, drop-head on overflow, max ttl 1 day)
      $amqpQueue = new AMQPQueue($this->getChannel());
      $amqpQueue->bind("{$this->exchangeName}.dead");
      $amqpQueue->setName("{$this->exchangeName}.unrouted");
      $amqpQueue->setFlags(AMQP_DURABLE);
      $amqpQueue->setArgument('x-max-length', 1000);
      $amqpQueue->setArgument('x-overflow', 'drop-head');
      $amqpQueue->setArgument('x-message-ttl', 86_400_000);
      $amqpQueue->declareQueue();
      $amqpQueue->bind("{$this->exchangeName}.unrouted");

      $ready = true;
    }
  }

  private function setupRoute(string $routingKey): void {
    if (isset($this->registeredRoutes[$routingKey]) === false) {
      // possibly unrouted key (at least not by courier)
      return;
    }

    foreach ($this->registeredRoutes[$routingKey] as $queueName => $status) {
      if ($status === true) {
        // already done
        continue;
      }

      $amqpQueue = $this->setupQueue($queueName);
      $amqpQueue->bind($this->exchangeName, $routingKey);
      $this->registeredRoutes[$routingKey][$queueName] = true;
    }
  }

  private function setupQueue(string $queueName): AMQPQueue {
    if (isset($this->amqpQueues[$queueName]) === false) {
      $amqpQueue = new AMQPQueue($this->getChannel());
      $amqpQueue->setName($queueName);
      $amqpQueue->setFlags(AMQP_DURABLE);
      $amqpQueue->setArgument('x-dead-letter-exchange', "{$this->exchangeName}.dead");
      $amqpQueue->declareQueue();
      $this->amqpQueues[$queueName] = $amqpQueue;
    }

    return $this->amqpQueues[$queueName];
  }

  public function __construct(
    AMQPConnection $amqpConnection,
    SerializerInterface $serializer = new JsonSerializer()
  ) {
    $this->amqpConnection = $amqpConnection;
    $this->serializer = $serializer;
  }

  public function addRoute(string $routingKey, string $queueName): self {
    echo 'Called: AmqpExtTransport::addRoute', PHP_EOL;
    echo 'Routing key: ', $routingKey, PHP_EOL;
    echo 'Queue name: ', $queueName, PHP_EOL;
    echo '====', PHP_EOL;

    $this->registeredRoutes[$routingKey] ??= [];
    if (in_array($queueName, $this->registeredRoutes[$routingKey], true) === false) {
      $this->registeredRoutes[$routingKey][$queueName] = false;
    }

    return $this;
  }

  public function publish(MessageInterface $message, string $routingKey): void {
    try {
      echo 'Called: AmqpExtTransport::publish', PHP_EOL;
      echo 'Routing key: ', $routingKey, PHP_EOL;
      echo 'Got message ', $message::class, PHP_EOL;
      echo 'Attributes: ', print_r($message->getAttributes(), true), PHP_EOL;
      echo 'Properties: ', print_r($message->getProperties(), true), PHP_EOL;
      echo 'Payload: ', print_r($message->getPayload(), true), PHP_EOL;
      echo '====', PHP_EOL;

      $this->setupServer();
      $this->setupRoute($routingKey);

      $this->amqpExchange->publish(
        $this->serializer->serialize($message),
        $routingKey,
        AMQP_NOPARAM,
        array_merge(
          [
            'content_encoding' => $this->serializer->getContentEncoding(),
            'content_type' => $this->serializer->getContentType(),
          ],
          $this->messageAttributes($message)
        )
      );
    } catch (AMQPException $exception) {
      throw new TransportException(
        'Failed to publish message: delivery exception',
        previous: $exception
      );
    }
  }

  public function collect(string $queueName): ?MessageInterface {
    try {
      $this->setupServer();
      $amqpQueue = $this->setupQueue($queueName);

      $envelope = $amqpQueue->get();
      if (($envelope instanceof AMQPEnvelope) === false) {
        return null;
      }

      if ($envelope->getContentEncoding() !== $this->serializer->getContentEncoding()) {
        throw new SerializerException(
          sprintf(
            'Cannot decode "%s" using "%s"',
            $envelope->getContentEncoding(),
            $this->serializer::class
          )
        );
      }

      if ($envelope->getContentType() !== $this->serializer->getContentType()) {
        throw new SerializerException(
          sprintf(
            'Cannot unserialize "%s" using "%s"',
            $envelope->getContentType(),
            $this->serializer::class
          )
        );
      }

      $message = $this->serializer->unserialize($envelope->getBody());
      $message->setProperties(
        [
          'consumerTag' => $envelope->getConsumerTag(),
          'deliveryTag' => $envelope->getDeliveryTag(),
          'exchangeName' => $envelope->getExchangeName(),
          'isRedelivery' => $envelope->isRedelivery(),
          'queueName' => $queueName,
          'routingKey' => $envelope->getRoutingKey()
        ]
      );

      return $message;
      // return $this->fromEnvelope($envelope);
    } catch (AMQPException $exception) {
      throw new TransportException(
        'Failed to collect message: delivery exception',
        previous: $exception
      );
    }
  }

  public function accept(MessageInterface $message): void {
    try {
      if ($message->hasProperty('deliveryTag') === false) {
        throw new TransportException(
          'Failed to accept message: missing delivery tag property'
        );
      }

      if ($message->hasProperty('queueName') === false) {
        throw new TransportException(
          'Failed to accept message: missing queue name property'
        );
      }

      $amqpQueue = $this->setupQueue($message->getProperty('queueName'));
      $amqpQueue->ack($message->getProperty('deliveryTag'));
    } catch (AMQPException $exception) {
      throw new TransportException(
        'Failed to accept message: delivery exception',
        previous: $exception
      );
    }
  }

  public function reject(MessageInterface $message, bool $requeue = false): void {
    try {
      if ($message->hasProperty('deliveryTag') === false) {
        throw new TransportException(
          'Failed to reject message: missing delivery tag property'
        );
      }

      if ($message->hasProperty('queueName') === false) {
        throw new TransportException(
          'Failed to reject message: missing queue name property'
        );
      }

      $amqpQueue = $this->setupQueue($message->getProperty('queueName'));
      $amqpQueue->reject($message->getProperty('deliveryTag'), $requeue ? AMQP_REQUEUE : AMQP_NOPARAM);
    } catch (AMQPException $exception) {
      throw new TransportException(
        'Failed to reject message: delivery exception',
        previous: $exception
      );
    }
  }
}
