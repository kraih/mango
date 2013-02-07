package Mango::Database;
use Mojo::Base -base;

use Mango::BSON 'bson_doc';
use Mango::Collection;

has [qw(mango name)];

sub collection {
  my ($self, $name) = @_;
  return Mango::Collection->new(db => $self, name => $name);
}

sub command {
  my ($self, $command) = (shift, shift);
  my $cb = ref $_[-1] eq 'CODE' ? pop : undef;
  $command = ref $command ? $command : bson_doc($command => 1, @_);

  # Non-blocking
  return $self->collection('$cmd')->find_one(
    $command => sub {
      my ($collection, $err, $doc) = @_;
      $self->$cb($err, $doc);
    }
  ) if $cb;

  # Blocking
  return $self->collection('$cmd')->find_one($command);
}

1;

=head1 NAME

Mango::Database - MongoDB database

=head1 SYNOPSIS

  use Mango::Database;

  my $db = Mango::Database->new(mango => $mango);
  my $collection = $db->collection('foo');

=head1 DESCRIPTION

L<Mango::Database> is a container for MongoDB databases used by L<Mango>.

=head1 ATTRIBUTES

L<Mango::Database> implements the following attributes.

=head2 mango

  my $mango = $db->mango;
  $db       = $db->mango(Mango->new);

L<Mango> object this database belongs to.

=head2 name

  my $name = $db->name;
  $db      = $db->name('bar');

Name of this database.

=head1 METHODS

L<Mango::Database> inherits all methods from L<Mojo::Base> and implements the
following new ones.

=head2 collection

  my $collection = $db->collection('foo');

Get L<Mango::Collection> object for collection.

=head2 command

  my $doc = $db->command($doc);
  my $doc = $db->command('getLastError', {w => 2});

Run command against database. You can also append a callback to run command
non-blocking.

  $db->command(('getLastError', {w => 2}) => sub {
    my ($db, $err, $doc) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head1 SEE ALSO

L<Mango>, L<Mojolicious::Guides>, L<http://mojolicio.us>.

=cut
