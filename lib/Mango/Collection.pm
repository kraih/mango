package Mango::Collection;
use Mojo::Base -base;

use Carp 'croak';
use Mango::BSON qw(bson_doc bson_oid bson_true);
use Mango::Cursor;

has [qw(db name)];

sub create {
  my $self = shift;
  my $cb   = ref $_[-1] eq 'CODE' ? pop : undef;
  my $doc  = bson_doc create => $self->name, %{shift // {}};

  # Non-blocking
  return $self->db->command($doc => sub { shift; $self->$cb(@_) }) if $cb;

  # Blocking
  return $self->db->command($doc);
}

sub drop {
  my ($self, $cb) = @_;

  # Non-blocking
  my $doc = bson_doc drop => $self->name;
  return $self->db->command($doc => sub { shift; $self->$cb(@_) }) if $cb;

  # Blocking
  return $self->db->command($doc);
}

sub ensure_index {
  my ($self, $spec) = (shift, shift);
  my $cb = ref $_[-1] eq 'CODE' ? pop : undef;
  my $doc = shift // {};

  $doc->{name} //= join '_', keys %$spec;
  $doc->{ns}  = $self->full_name;
  $doc->{key} = $spec;

  # Non-blocking
  my $collection = $self->db->collection('system.indexes');
  return $collection->insert($doc => sub { shift; $self->$cb(@_) }) if $cb;

  # Blocking
  $collection->insert($doc);
}

sub find {
  my ($self, $query) = @_;
  return Mango::Cursor->new(collection => $self, query => $query);
}

sub find_one {
  my ($self, $query) = @_;
  $query = {_id => $query} if ref $query eq 'Mango::BSON::ObjectID';
  my $cb = ref $_[-1] eq 'CODE' ? pop : undef;

  # Non-blocking
  my $cursor = $self->find($query)->limit(-1);
  return $cursor->next(sub { shift; $self->$cb(@_) }) if $cb;

  # Blocking
  return $cursor->next;
}

sub full_name { join '.', $_[0]->db->name, $_[0]->name }

sub insert {
  my ($self, $docs) = @_;
  $docs = [$docs] unless ref $docs eq 'ARRAY';
  my $cb = ref $_[-1] eq 'CODE' ? pop : undef;

  # Make sure all documents have ids
  my @ids = map { $_->{_id} //= bson_oid } @$docs;

  # Non-blocking
  my $mango = $self->db->mango;
  return $mango->insert(
    ($self->full_name, {}, @$docs) => sub {
      my ($mango, $err, $reply) = @_;
      $err ||= $mango->protocol->command_error($reply);
      $self->$cb($err, @ids > 1 ? \@ids : $ids[0]);
    }
  ) if $cb;

  # Blocking
  my $reply = $mango->insert($self->full_name, {}, @$docs);
  if (my $err = $mango->protocol->command_error($reply)) { croak $err }
  return @ids > 1 ? \@ids : $ids[0];
}

sub remove {
  my $self  = shift;
  my $query = ref $_[0] eq 'CODE' ? {} : shift // {};
  my $flags = ref $_[0] eq 'CODE' ? {} : shift // {};
  $flags->{single_remove} = delete $flags->{single};
  return $self->_handle('delete', $flags, $query, @_);
}

sub update {
  my ($self, $query, $update) = (shift, shift, shift);
  my $flags = ref $_[0] eq 'CODE' ? {} : shift // {};
  $flags->{multi_update} = delete $flags->{multi};
  return $self->_handle('update', $flags, $query, $update, @_);
}

sub _handle {
  my ($self, $method) = (shift, shift);
  my $cb = ref $_[-1] eq 'CODE' ? pop : undef;

  # Non-blocking
  my $mango = $self->db->mango;
  return $mango->$method(
    ($self->full_name, @_) => sub {
      my ($mango, $err, $reply) = @_;
      $err ||= $mango->protocol->command_error($reply);
      $self->$cb($err, $reply->{docs}[0]);
    }
  ) if $cb;

  # Blocking
  my $reply = $mango->$method($self->full_name, @_);
  if (my $err = $mango->protocol->command_error($reply)) { croak $err }
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

=head2 create

  $collection->create;
  $collection->create({capped => bson_true, max => 5, size => 10000});

Create collection. You can also append a callback to perform operation
non-blocking.

  $collection->create({capped => bson_true, max => 5, size => 10000} => sub {
    my ($collection, $err) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 drop

  my $doc = $collection->drop;

Drop collection. You can also append a callback to perform operation
non-blocking.

  $collection->drop(sub {
    my ($collection, $err, $doc) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 ensure_index

  $collection->ensure_index(bson_doc(foo => 1, bar => -1));
  $collection->ensure_index({foo => 1});
  $collection->ensure_index({foo => 1}, {unique => bson_true});

Make sure an index exists, the order of keys matters for compound indexes. You
can also append a callback to perform operation non-blocking.

  $collection->ensure_index(({foo => 1}, {unique => bson_true}) => sub {
    my ($collection, $err) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

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
