
# Mango [![Build Status](https://travis-ci.org/kraih/mango.svg?branch=master)](https://travis-ci.org/kraih/mango)

  Pure-Perl non-blocking I/O MongoDB driver, optimized for use with the
  [Mojolicious](http://mojolicio.us) real-time web framework.

```perl
use Mojolicious::Lite;
use Mango;
use Mango::BSON ':bson';

my $uri = 'mongodb://<user>:<pass>@<server>/<database>';
helper mango => sub { state $mango = Mango->new($uri) };

# Store and retrieve information non-blocking
get '/' => sub {
  my $c = shift;

  my $collection = $c->mango->db->collection('visitors');
  my $ip         = $c->tx->remote_address;

  # Store information about current visitor
  $collection->insert({when => bson_time, from => $ip} => sub {
    my ($collection, $err, $oid) = @_;

    return $c->reply->exception($err) if $err;

    # Retrieve information about previous visitors
    $collection->find->sort({when => -1})->fields({_id => 0})->all(sub {
      my ($collection, $err, $docs) = @_;

      return $c->reply->exception($err) if $err;

      # And show it to current visitor
      $c->render(json => $docs);
    });
  });
};

app->start;
```

## Installation

  All you need is a oneliner, it takes less than a minute.

    $ curl -L cpanmin.us | perl - -n Mango

  We recommend the use of a [Perlbrew](http://perlbrew.pl) environment.
