<?php
declare(strict_types = 1);

namespace Courier\Transport;

use AMQPChannel;
use AMQPConnection;
use AMQPEnvelope;
use AMQPException;
use AMQPExchange;
use AMQPQueue;
use Courier\Exception\TransportException;
use Courier\Message\Envelope;
use Courier\Message\EnvelopeDeliveryModeEnum;
use Courier\Message\EnvelopePriorityEnum;
use DateTimeImmutable;
use Nyholm\Dsn\DsnParser;

final class AmqpTransport implements TransportInterface {
  /**
   * Main exchange name
   */
  private const EXCHANGE_NAME = 'courier.bus';
  /**
   * Number of messages to be prefetched
   */
  private const PREFETCH_COUNT = 25;

  private AMQPConnection $amqpConnection;
  private ?AMQPChannel $amqpChannel = null;
  private ?AMQPExchange $amqpExchange = null;
  private ?AMQPQueue $amqpQueue = null;
  private int $prefetchCount;

  public static function fromDsn(string $dsn): self {
    $dsn = DsnParser::parse($dsn);

    if (in_array($dsn->getScheme(), ['amqp', 'amqp-ext'], true) === false) {
      throw new TransportException(
        sprintf(
          'Invalid DSN scheme "%s"',
          $dsn->getScheme()
        )
      );
    }

    return new self(
      new AMQPConnection(
        [
          'host'     => $dsn->getHost() ?? 'localhost',
          'port'     => $dsn->getPort() ?? 5672,
          'vhost'    => $dsn->getPath() ?? '/',
          'login'    => $dsn->getUser() ?? 'guest',
          'password' => $dsn->getPassword() ?? 'guest'
        ]
      )
    );
  }

  public function __construct(AMQPConnection $amqpConnection) {
    $this->amqpConnection = $amqpConnection;
    $this->prefetchCount  = self::PREFETCH_COUNT;
  }

  public function init(): void {
    try {
      if ($this->amqpConnection->isConnected() === false) {
        $this->amqpConnection->connect();
      }

      if ($this->amqpChannel === null) {
        $this->amqpChannel = new AMQPChannel($this->amqpConnection);
        $this->amqpChannel->setPrefetchCount($this->prefetchCount);
      }

      if ($this->amqpExchange === null) {
        $this->amqpExchange = new AMQPExchange($this->amqpChannel);
        $this->amqpExchange->setName(self::EXCHANGE_NAME);
        $this->amqpExchange->setType(AMQP_EX_TYPE_DIRECT);
        $this->amqpExchange->setFlags(AMQP_DURABLE);
        $this->amqpExchange->declareExchange();
      }

      if ($this->amqpQueue === null) {
        $this->amqpQueue = new AMQPQueue($this->amqpChannel);
      }
    } catch (AMQPException $exception) {
      throw new TransportException(
        $exception->getMessage(),
        $exception->getCode(),
        $exception
      );
    }
  }
  /**
   * Return the attribute value
   */
  public function getAttribute(string $name): mixed {
    try {
      if ($this->amqpQueue === null) {
        throw new TransportException('Transport has not been initialized');
      }

      return $this->amqpQueue->getArgument($name);
    } catch (AMQPException $exception) {
      throw new TransportException(
        $exception->getMessage(),
        $exception->getCode(),
        $exception
      );
    }
  }
  /**
   * Set the attribute value
   */
  public function setAttribute(string $name, mixed $value): static {
    try {
      if ($this->amqpQueue === null) {
        throw new TransportException('Transport has not been initialized');
      }

      $this->amqpQueue->setArgument($name, $value);

      return $this;
    } catch (AMQPException $exception) {
      throw new TransportException(
        $exception->getMessage(),
        $exception->getCode(),
        $exception
      );
    }
  }

  public function getPrefetchCount(): int {
    return $this->prefetchCount;
  }

  public function setPrefetchCount(int $prefetchCount): static {
    if ($prefetchCount < 1) {
      throw new InvalidArgumentException('$prefetchCount must be greater than or equal to 1');
    }

    $this->prefetchCount = $prefetchCount;
    if ($this->amqpChannel !== null) {
      $this->amqpChannel->setPrefetchCount($prefetchCount);
    }


    return $this;
  }

  public function bindQueue(string $queueName, string $routingKey): void {
    try {
      if ($this->amqpQueue === null) {
        throw new TransportException('Transport has not been initialized');
      }

      $this->amqpQueue->setName($queueName);
      $this->amqpQueue->setFlags(AMQP_DURABLE);
      $this->amqpQueue->declareQueue();
      $this->amqpQueue->bind(self::EXCHANGE_NAME, $routingKey);
    } catch (AMQPException $exception) {
      throw new TransportException(
        $exception->getMessage(),
        $exception->getCode(),
        $exception
      );
    }
  }

  public function unbindQueue(string $queueName, string $routingKey): void {
    try {
      if ($this->amqpQueue === null) {
        throw new TransportException('Transport has not been initialized');
      }

      $this->amqpQueue->setName($queueName);
      $this->amqpQueue->setFlags(AMQP_DURABLE);
      $this->amqpQueue->declareQueue();
      $this->amqpQueue->bind(self::EXCHANGE_NAME, $routingKey);
    } catch (AMQPException $exception) {
      throw new TransportException(
        $exception->getMessage(),
        $exception->getCode(),
        $exception
      );
    }
  }

  public function pending(string $queueName): int {
    try {
      if ($this->amqpQueue === null) {
        throw new TransportException('Transport has not been initialized');
      }

      $this->amqpQueue->setName($queueName);
      $this->amqpQueue->setFlags(AMQP_DURABLE);

      return $this->amqpQueue->declareQueue();
    } catch (AMQPException $exception) {
      throw new TransportException(
        $exception->getMessage(),
        $exception->getCode(),
        $exception
      );
    }
  }

  public function purge(string $queueName): void {
    try {
      if ($this->amqpQueue === null) {
        throw new TransportException('Transport has not been initialized');
      }

      $this->amqpQueue->setName($queueName);
      $this->amqpQueue->setFlags(AMQP_DURABLE);
      $this->amqpQueue->declareQueue();
      $this->amqpQueue->purge();
    } catch (AMQPException $exception) {
      throw new TransportException(
        $exception->getMessage(),
        $exception->getCode(),
        $exception
      );
    }
  }

  public function subscribe(string $queueName): void {
    try {
      if ($this->amqpQueue === null) {
        throw new TransportException('Transport has not been initialized');
      }

      $consumerTag = sprintf(
        'courier.consumer:%s',
        substr($queueName, strrpos($queueName, ':') + 1)
      );

      $this->amqpQueue->setName($queueName);
      $this->amqpQueue->setFlags(AMQP_DURABLE);
      $this->amqpQueue->declareQueue();
      $this->amqpQueue->consume(null, AMQP_NOPARAM, $consumerTag);
    } catch (AMQPException $exception) {
      throw new TransportException(
        $exception->getMessage(),
        $exception->getCode(),
        $exception
      );
    }
  }

  public function consume(string $queueName, callable $consumer, callable $stop): int {
    try {
      if ($this->amqpQueue === null) {
        throw new TransportException('Transport has not been initialized');
      }

      $consumerTag = sprintf(
        'courier.consumer:%s',
        substr($queueName, strrpos($queueName, ':') + 1)
      );

      $accepted = 0;
      $rejected = 0;
      $requeued = 0;
      $consumed = 0;

      $this->amqpQueue->setName($queueName);
      $this->amqpQueue->setFlags(AMQP_DURABLE);
      $this->amqpQueue->declareQueue();
      $this->amqpQueue->consume(
        static function (
          AMQPEnvelope $amqpEnvelope,
          AMQPQueue $amqpQueue
        ) use (
          &$accepted,
          &$rejected,
          &$requeued,
          &$consumed,
          $consumer,
          $stop
        ): bool {
          $envelope = Envelope::fromArray(
            $amqpEnvelope->getBody(),
            [
              'appId'           => $amqpEnvelope->getAppId(),
              'body'            => $amqpEnvelope->getBody(),
              'contentEncoding' => $amqpEnvelope->getContentEncoding(),
              'contentType'     => $amqpEnvelope->getContentType(),
              'correlationId'   => $amqpEnvelope->getCorrelationId(),
              'deliveryMode'    => EnvelopeDeliveryModeEnum::tryFrom($amqpEnvelope->getDeliveryMode()),
              'deliveryTag'     => $amqpEnvelope->getDeliveryTag(),
              'expiration'      => $amqpEnvelope->getExpiration(),
              'headers'         => $amqpEnvelope->getHeaders(),
              'isRedelivery'    => $amqpEnvelope->isRedelivery(),
              'messageId'       => $amqpEnvelope->getMessageId(),
              'priority'        => EnvelopePriorityEnum::tryFrom($amqpEnvelope->getPriority()),
              'replyTo'         => $amqpEnvelope->getReplyTo(),
              'timestamp'       => (new DateTimeImmutable())->setTimestamp((int)$amqpEnvelope->getTimestamp()),
              'type'            => $amqpEnvelope->getType(),
              'userId'          => $amqpEnvelope->getUserId()
            ]
          );
          switch ($consumer($envelope, $amqpQueue->getName())) {
            case TransportResultEnum::ACCEPT:
              $amqpQueue->ack($amqpEnvelope->getDeliveryTag());
              $accepted++;
              break;
            case TransportResultEnum::REJECT:
              $amqpQueue->reject($amqpEnvelope->getDeliveryTag(), AMQP_NOPARAM);
              $rejected++;
              break;
            case TransportResultEnum::REQUEUE:
              $amqpQueue->reject($amqpEnvelope->getDeliveryTag(), AMQP_REQUEUE);
              $requeued++;
              break;
          }

          $consumed++;

          // must return true to keep consuming
          return $stop($accepted, $rejected, $requeued, $consumed) === false;
        },
        AMQP_NOPARAM,
        $consumerTag
      );

      return $consumed;
    } catch (AMQPException $exception) {
      throw new TransportException(
        $exception->getMessage(),
        $exception->getCode(),
        $exception
      );
    }
  }

  public function send(string $routingKey, Envelope $envelope): void {
    try {
      if ($this->amqpExchange === null) {
        throw new TransportException('Transport has not been initialized');
      }

      $this->amqpExchange->publish(
        $envelope->getBody(),
        $routingKey,
        AMQP_NOPARAM,
        [
          'app_id'           => $envelope->getAppId(),
          'content_encoding' => $envelope->getContentEncoding(),
          'content_type'     => $envelope->getContentType(),
          'correlation_id'   => $envelope->getCorrelationId(),
          'delivery_mode'    => $envelope->getDeliveryMode()->value,
          'expiration'       => $envelope->getExpiration(),
          'headers'          => $envelope->getHeaders(),
          'message_id'       => $envelope->getMessageId(),
          'priority'         => $envelope->getPriority()->value,
          'reply_to'         => $envelope->getReplyTo(),
          'timestamp'        => $envelope->getTimestamp()?->getTimestamp(),
          'type'             => $envelope->getType(),
          'user_id'          => $envelope->getUserId()
        ]
      );
    } catch (AMQPException $exception) {
      throw new TransportException(
        $exception->getMessage(),
        $exception->getCode(),
        $exception
      );
    }
  }

  public function accept(Envelope $envelope): void {
    try {
      if ($this->amqpQueue === null) {
        throw new TransportException('Transport has not been initialized');
      }

      $this->amqpQueue->ack($envelope->getDeliveryTag());
    } catch (AMQPException $exception) {
      throw new TransportException(
        $exception->getMessage(),
        $exception->getCode(),
        $exception
      );
    }
  }

  public function reject(Envelope $envelope, bool $requeue = false): void {
    try {
      if ($this->amqpQueue === null) {
        throw new TransportException('Transport has not been initialized');
      }

      $this->amqpQueue->reject($envelope->getDeliveryTag(), $requeue ? AMQP_REQUEUE : AMQP_NOPARAM);
    } catch (AMQPException $exception) {
      throw new TransportException(
        $exception->getMessage(),
        $exception->getCode(),
        $exception
      );
    }
  }
}
