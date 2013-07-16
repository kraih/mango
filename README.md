
# Mango [![Build Status](https://secure.travis-ci.org/kraih/mango.png)](http://travis-ci.org/kraih/mango)

  Pure-Perl non-blocking I/O MongoDB driver, optimized for use with the
  [Mojolicious](http://mojolicio.us) real-time web framework.

    use Mojolicious::Lite;
    use Mango;
    use Mango::BSON ':bson';

    my $uri = 'mongodb://<user>:<pass>@<server>/<database>';
    helper mango => sub { state $mango = Mango->new($uri) };

    # Store and retrieve information non-blocking
    get '/' => sub {
      my $self = shift;

      my $collection = $self->mango->db->collection('visitors');
      my $ip         = $self->tx->remote_address;

      # Store information about current visitor
      $collection->insert({when => bson_time, from => $ip} => sub {
        my ($collection, $err, $oid) = @_;

        return $self->render_exception if $err;

        # Retrieve information about previous visitors
        $collection->find({})->sort({when => -1})->fields({_id => 0})->all(sub {
          my ($collection, $err, $docs) = @_;

          return $self->render_exception if $err;

          # And show it to current visitor
          $self->render(json => $docs);
        });
      });
    };

    app->start;

## Installation

  All you need is a oneliner, it takes less than a minute.

    $ curl -L cpanmin.us | perl - -n  Mango

  We recommend the use of a [Perlbrew](http://perlbrew.pl) environment.
