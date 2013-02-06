
# Mango [![Build Status](https://secure.travis-ci.org/kraih/mango.png)](http://travis-ci.org/kraih/mango)

  Pure-Perl non-blocking I/O MongoDB client, optimized for use with the
  [Mojolicious](http://mojolicio.us) real-time web framework.

    use Mango;
    my $mango = Mango->new('mongodb://localhost:27017');

    # Insert document
    my $oid = $mango->db('test')->collection('foo')->insert({bar => 'baz'});

    # Find document
    use Mango::BSON ':bson';
    my $doc = $mango->db('test')->collection('foo')->find_one({bar => 'baz'});
    say $doc->{bar};

    # Update document with special BSON type
    use Mango::BSON ':bson';
    $mango->db('test')->collection('foo')
      ->update({bar => 'baz'}, {bar => bson_true});

    # Remove document with special BSON type
    use Mango::BSON ':bson';
    $mango->db('test')->collection('foo')->remove({bar => bson_true});

    # Find documents non-blocking (does work inside a running event loop)
    my $delay = Mojo::IOLoop->delay(sub {
      my ($delay, @docs) = @_;
      ...
    });
    for my $name (qw(foo bar)) {
      $delay->begin;
      $mango->db('test')->collection('users')->find({name => $name})->all(sub {
        my ($cursor, $err, $docs) = @_;
        $delay->end(@$docs);
      });
    }
    $delay->wait unless Mojo::IOLoop->is_running;

## Installation

  All you need is a oneliner, it takes less than a minute.

    $ curl -L cpanmin.us | perl - -n  Mango

  We recommend the use of a [Perlbrew](http://perlbrew.pl) environment.
