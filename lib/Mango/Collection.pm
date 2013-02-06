package Mango::Collection;
use Mojo::Base -base;

use Carp 'croak';
use Mango::BSON 'bson_oid';
use Mango::Cursor;

has [qw(db name)];

sub find {
  my ($self, $query) = @_;
  return Mango::Cursor->new(collection => $self, query => $query);
}

sub find_one {
  my ($self, $query) = @_;
  $query = {_id => $query} if ref $query eq 'Mango::BSON::ObjectID';

  # Non-blocking
  my $cb = ref $_[-1] eq 'CODE' ? pop : undef;
  return $self->find($query)->limit(-1)->next(
    sub {
      my ($cursor, $err, $doc) = @_;
      $self->$cb($err, $doc);
    }
  ) if $cb;

  # Blocking
  return $self->find($query)->limit(-1)->next;
}

sub full_name { join '.', $_[0]->db->name, $_[0]->name }

sub insert {
  my ($self, $docs) = @_;
  $docs = [$docs] unless ref $docs eq 'ARRAY';

  # Make sure all documents have ids
  my @ids = map { $_->{_id} //= bson_oid } @$docs;

  # Non-blocking
  my $cb = ref $_[-1] eq 'CODE' ? pop : undef;
  return $self->db->mango->insert(
    ($self->full_name, {}, @$docs) => sub {
      my ($mango, $err, $reply) = @_;
      $err ||= _error($reply);
      $self->$cb($err, @ids > 1 ? \@ids : $ids[0]);
    }
  ) if $cb;

  # Blocking
  my $reply = $self->db->mango->insert($self->full_name, {}, @$docs);
  if (my $err = _error($reply)) { croak $err }
  return @ids > 1 ? \@ids : $ids[0];
}

sub remove {
  my $self = shift;
  my $query = ref $_[0] eq 'CODE' ? {} : shift // {};
  return $self->_handle('delete', {}, $query, @_);
}

sub update {
  my ($self, $query, $update) = (shift, shift, shift);
  return $self->_handle('update', {}, $query, $update, @_);
}

sub _error { $_[0]->[5][0]{ok} ? $_[0]->[5][0]{err} : $_[0]->[5][0]{errmsg} }

sub _handle {
  my ($self, $method) = (shift, shift);

  # Non-blocking
  my $cb = ref $_[-1] eq 'CODE' ? pop : undef;
  return $self->db->mango->$method(
    ($self->full_name, @_) => sub {
      my ($mango, $err, $reply) = @_;
      $err ||= _error($reply);
      $self->$cb($err, $reply->[5][0]);
    }
  ) if $cb;

  # Blocking
  my $reply = $self->db->mango->$method($self->full_name, @_);
  if (my $err = _error($reply)) { croak $err }
  return $reply->[5][0];
}

1;

=head1 NAME

Mango::Collection - MongoDB collection

=head1 SYNOPSIS

  use Mango::Collection;

  my $collection = Mango::Collection->new(db => $db);
  my $cursor     = $collection->find({foo => 'bar'});

=head1 DESCRIPTION

L<Mango::Collection> is a container for MongoDB collections used by
L<Mango::Database>.

=head1 ATTRIBUTES

L<Mango::Collection> implements the following attributes.

=head2 db

  my $db      = $collection->db;
  $collection = $collection->db(Mango::Database->new);

L<Mango::Database> object this collection belongs to.

=head2 name

  my $name    = $collection->name;
  $collection = $collection->name('bar');

Name of this collection.

=head1 METHODS

L<Mango::Collection> inherits all methods from L<Mojo::Base> and implements
the following new ones.

=head2 find

  my $cursor = $collection->find({foo => 'bar'});

Get L<Mango::Cursor> object for query.

=head2 find_one

  my $doc = $collection->find_one({foo => 'bar'});
  my $doc = $collection->find_one($oid);

Find one document. You can also append a callback to perform operation
non-blocking.

  $collection->find_one({foo => 'bar'} => sub {
    my ($collection, $err, $doc) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 full_name

  my $name = $collection->full_name;

Full name of this collection.

=head2 insert

  my $oid  = $collection->insert({foo => 'bar'});
  my $oids = $collection->insert([{foo => 'bar'}, {baz => 'yada'}]);

Insert one or more documents into collection. You can also append a callback
to perform operation non-blocking.

  $collection->insert({foo => 'bar'} => sub {
    my ($collection, $err, $oid) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 remove

  my $doc = $collection->remove;
  my $doc = $collection->remove({foo => 'bar'});

Remove documents from collection. You can also append a callback to perform
operation non-blocking.

  $collection->remove({foo => 'bar'} => sub {
    my ($collection, $err, $doc) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 update

  my $doc = $collection->update({foo => 'bar'}, {foo => 'baz'});

Update document in collection. You can also append a callback to perform
operation non-blocking.

  $collection->update(({foo => 'bar'}, {foo => 'baz'}) => sub {
    my ($collection, $err, $doc) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head1 SEE ALSO

L<Mango>, L<Mojolicious::Guides>, L<http://mojolicio.us>.

=cut
