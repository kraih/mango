
# Mango

  Pure-Perl non-blocking I/O MongoDB client, optimized for use with the
  [Mojolicious](http://mojolicio.us) real-time web framework.

    use Mango;
    my $mango = Mango->new('mongodb://localhost:27017');

    # Insert document
    my $oid = $mango->db('test')->collection('foo')->insert({bar => 'baz'});

    # Find document
    my $doc = $mango->db('test')->collection('foo')->find_one({bar => 'baz'});
    say $doc->{bar};

    # Update document
    $mango->db('test')->collection('foo')
      ->update({bar => 'baz'}, {bar => 'yada'});

    # Remove document
    $mango->db('test')->collection('foo')->remove({bar => 'yada'});

    # Insert document with special BSON types
    use Mango::BSON ':bson';
    my $oid = $mango->db('test')->collection('foo')
      ->insert({data => bson_bin("\x00\x01"), now => bson_time});

    # Blocking parallel find (does not work inside a running event loop)
    my $delay = Mojo::IOLoop->delay;
    for my $name (qw(sri marty)) {
      $delay->begin;
      $mango->db('test')->collection('users')->find({name => $name})->all(sub {
        my ($cursor, $err, $docs) = @_;
        $delay->end(@$docs);
      });
    }
    my @docs = $delay->wait;

    # Non-blocking parallel find (does work inside a running event loop)
    my $delay = Mojo::IOLoop->delay(sub {
      my ($delay, @docs) = @_;
      ...
    });
    for my $name (qw(sri marty)) {
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
