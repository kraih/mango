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
  my $cb = ref $_[-1] eq 'CODE' ? pop : undef;

  # Non-blocking
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
  my $cb = ref $_[-1] eq 'CODE' ? pop : undef;

  # Make sure all documents have ids
  my @ids = map { $_->{_id} //= bson_oid } @$docs;

  # Non-blocking
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
  my $self    = shift;
  my $query   = ref $_[0] eq 'CODE' ? {} : shift // {};
  my $options = ref $_[0] eq 'CODE' ? {} : shift // {};
  my $flags   = $options->{single} ? {single_remove => 1} : {};
  return $self->_handle('delete', $flags, $query, @_);
}

sub update {
  my ($self, $query, $update) = (shift, shift, shift);
  my $options = ref $_[0] eq 'CODE' ? {} : shift // {};

  my $flags = {};
  $flags->{upsert}       = $options->{upsert};
  $flags->{multi_update} = $options->{multi};

  return $self->_handle('update', $flags, $query, $update, @_);
}

sub _error {
  my $doc = shift->{docs}[0];
  return $doc->{ok} ? $doc->{err} : $doc->{errmsg};
}

sub _handle {
  my ($self, $method) = (shift, shift);
  my $cb = ref $_[-1] eq 'CODE' ? pop : undef;

  # Non-blocking
  return $self->db->mango->$method(
    ($self->full_name, @_) => sub {
      my ($mango, $err, $reply) = @_;
      $err ||= _error($reply);
      $self->$cb($err, $reply->{docs}[0]);
    }
  ) if $cb;

  # Blocking
  my $reply = $self->db->mango->$method($self->full_name, @_);
  if (my $err = _error($reply)) { croak $err }
  return $reply->{docs}[0];
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
  my $doc = $collection->remove({foo => 'bar'}, {single => 1});

Remove documents from collection. You can also append a callback to perform
operation non-blocking.

  $collection->remove(({foo => 'bar'}, {single => 1}) => sub {
    my ($collection, $err, $doc) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

These options are currently available:

=over 2

=item single

Remove only one document.

=back

=head2 update

  my $doc = $collection->update({foo => 'bar'}, {foo => 'baz'});
  my $doc = $collection->update({foo => 'bar'}, {foo => 'baz'}, {multi => 1});

Update document in collection. You can also append a callback to perform
operation non-blocking.

  $collection->update(({foo => 'bar'}, {foo => 'baz'}, {multi => 1}) => sub {
    my ($collection, $err, $doc) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

These options are currently available:

=over 2

=item multi

Update more than one document.

=item upsert

Insert document if none could be updated.

=back

=head1 SEE ALSO

L<Mango>, L<Mojolicious::Guides>, L<http://mojolicio.us>.

=cut
