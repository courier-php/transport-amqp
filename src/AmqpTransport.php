<?php
declare(strict_types = 1);

namespace Courier\Transports;

use AMQPConnection;
use Composer\InstalledVersions;
use Courier\Contracts\Transports\TransportInterface;
use Courier\Exceptions\TransportException;
use Nyholm\Dsn\Configuration\Url;
use Nyholm\Dsn\DsnParser;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use RuntimeException;

class AmqpTransport {
  private static function buildAmqpExt(Url $dsn): AmqpExtTransport {
    return new AmqpExtTransport(
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

  private static function buildAmqpLib(Url $dsn): AmqpLibTransport {
    return new AmqpLibTransport(
      new AmqpLibTransport(
        new AMQPStreamConnection(
          $dsn->getHost() ?? 'localhost',
          $dsn->getPort() ?? 5672,
          $dsn->getUser() ?? 'guest',
          $dsn->getPassword() ?? 'guest',
          $dsn->getPath() ?? '/',
        )
      )
    );
  }


  public static function fromDsn(string $dsn): TransportInterface {
    $dsn = DsnParser::parse($dsn);

    if (in_array($dsn->getScheme(), ['amqp', 'amqp-ext', 'amqp-lib'], true) === false) {
      throw new TransportException(
        sprintf(
          'Invalid DSN scheme "%s"',
          $dsn->getScheme()
        )
      );
    }

    switch ($dsn->getScheme()) {
      case 'amqp-ext':
        if (extension_loaded('amqp') === false) {
          throw new RuntimeException('To use "AmqpExtTransport" (amqp-ext://), the "php-amqp" extension must be loaded');
        }

        return self::buildAmqpExt($dsn);

      case 'amqp-lib':
        if (InstalledVersions::isInstalled('php-amqplib/php-amqplib') === false) {
          throw new RuntimeException('To use "AmqpLibTransport" (amqp-lib://), the "php-amqplib" library must be installed');
        }

        return self::buildAmqpLib($dsn);

      case 'amqp':
        if (extension_loaded('amqp') === true) {
          return self::buildAmqpExt($dsn);
        }

        if (InstalledVersions::isInstalled('php-amqplib/php-amqplib') === true) {
          return self::buildAmqpLib($dsn);
        }

        throw new RuntimeException('Could not find neither the "php-amqp" extension nor the "php-amqplib" library');
    }
  }
}
