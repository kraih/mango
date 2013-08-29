package Mango::Database;
use Mojo::Base -base;

use Carp 'croak';
use Mango::BSON qw(bson_code bson_doc);
use Mango::Collection;
use Mango::GridFS;

has [qw(mango name)];

sub collection {
  my ($self, $name) = @_;
  return Mango::Collection->new(db => $self, name => $name);
}

sub collection_names {
  my ($self, $cb) = @_;

  my $len        = length $self->name;
  my $collection = $self->collection('system.namespaces');

  # Non-blocking
  return $collection->find->all(
    sub {
      my ($cursor, $err, $docs) = @_;
      $self->$cb($err, [map { substr $_->{name}, $len + 1 } @$docs]);
    }
  ) if $cb;

  # Blocking
  my $docs = $collection->find->all;
  return [map { substr $_->{name}, $len + 1 } @$docs];
}

sub command {
  my ($self, $command) = (shift, shift);
  my $cb = ref $_[-1] eq 'CODE' ? pop : undef;
  $command = ref $command ? $command : bson_doc($command => 1, @_);

  # Non-blocking
  my $collection = $self->collection('$cmd');
  my $protocol   = $self->mango->protocol;
  return $collection->find_one(
    $command => sub {
      my ($collection, $err, $doc) = @_;
      $err ||= $protocol->command_error({docs => [$doc]});
      $self->$cb($err, $doc // {});
    }
  ) if $cb;

  # Blocking
  my $doc = $collection->find_one($command);
  if (my $err = $protocol->command_error({docs => [$doc]})) { croak $err }
  return $doc;
}

sub gridfs { Mango::GridFS->new(db => shift) }

sub stats { shift->command(bson_doc(dbstats => 1), @_) }

1;

=encoding utf8

=head1 NAME

Mango::Database - MongoDB database

=head1 SYNOPSIS

  use Mango::Database;

  my $db = Mango::Database->new(mango => $mango);
  my $collection = $db->collection('foo');
  my $gridfs     = $db->gridfs;

=head1 DESCRIPTION

L<Mango::Database> is a container for MongoDB databases used by L<Mango>.

=head1 ATTRIBUTES

L<Mango::Database> implements the following attributes.

=head2 mango

  my $mango = $db->mango;
  $db       = $db->mango(Mango->new);

L<Mango> object this database belongs to. Note that this reference is usually
weakened, so the L<Mango> object needs to be referenced elsewhere as well.

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

=head2 collection_names

  my $names = $db->collection_names;

Names of all collections in this database. You can also append a callback to
perform operation non-blocking.

  $db->collection_names(sub {
    my ($db, $err, $names) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 command

  my $doc = $db->command(bson_doc(getLastError => 1, w => 2));
  my $doc = $db->command('getLastError', w => 2);

Run command against database. You can also append a callback to run command
non-blocking.

  $db->command(('getLastError', w => 2) => sub {
    my ($db, $err, $doc) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 gridfs

  my $gridfs = $db->gridfs;

Get L<Mango::GridFS> object.

=head2 stats

  my $stats = $db->stats;

Get database statistics. You can also append a callback to perform operation
non-blocking.

  $db->stats(sub {
    my ($db, $err, $stats) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head1 SEE ALSO

L<Mango>, L<Mojolicious::Guides>, L<http://mojolicio.us>.

=cut
