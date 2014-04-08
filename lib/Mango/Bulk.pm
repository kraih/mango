package Mango::Bulk;
use Mojo::Base -base;

use Carp 'croak';
use Mango::BSON qw(bson_doc bson_encode bson_oid bson_raw);
use Mojo::IOLoop;

has 'collection';
has ordered => 1;

sub execute {
  my ($self, $cb) = @_;

  # Full results shared with all operations
  my $full = {upserted => [], writeConcernErrors => [], writeErrors => []};
  $full->{$_} = 0 for qw(nInserted nMatched nModified nRemoved nUpserted);

  # Non-blocking
  if ($cb) {
    return Mojo::IOLoop->next_tick(sub { shift; $self->$cb(undef, $full) })
      unless my $group = shift @{$self->{ops}};
    return $self->_next($group, $full, $cb);
  }

  # Blocking
  my $db       = $self->collection->db;
  my $protocol = $db->mango->protocol;
  while (my $group = shift @{$self->{ops}}) {
    my ($type, $offset, $command) = $self->_group($group);
    _merge($type, $offset, $full, $db->command($command));
    if (my $err = $protocol->write_error($full)) { croak $err }
  }

  return $full;
}

sub find { shift->_set(query => shift) }

sub insert {
  my ($self, $doc) = @_;
  $doc->{_id} //= bson_oid;
  return $self->_op(insert => $doc);
}

sub remove     { shift->_remove(0) }
sub remove_one { shift->_remove(1) }

sub update     { shift->_update(\1, @_) }
sub update_one { shift->_update(\0, @_) }

sub upsert { shift->_set(upsert => 1) }

sub _group {
  my ($self, $group) = @_;

  my ($type, $offset) = splice @$group, 0, 2;
  my $collection = $self->collection;
  return $type, $offset, bson_doc $type => $collection->name,
    $type eq 'insert' ? 'documents' : "${type}s" => $group,
    ordered => $self->ordered ? \1 : \0,
    writeConcern => $collection->db->build_write_concern;
}

sub _merge {
  my ($type, $offset, $full, $result) = @_;

  # Insert
  if ($type eq 'insert') { $full->{nInserted} += $result->{n} }

  # Update
  elsif ($type eq 'update') {
    $full->{nModified} += $result->{n};

    # Upsert
    if (my $upserted = $result->{upserted}) {
      push @{$full->{upserted}}, map { $_->{index} += $offset; $_ } @$upserted;
      $full->{nUpserted} += @$upserted;
      $full->{nMatched}  += $result->{n} - @$upserted;
    }

    else { $full->{nMatched} += $result->{n} }
  }

  # Delete
  elsif ($type eq 'delete') { $full->{nRemoved} += $result->{n} }

  # Errors
  push @{$full->{writeConcernErrors}}, $result->{writeConcernError}
    if $result->{writeConcernError};
  push @{$full->{writeErrors}},
    map { $_->{index} += $offset; $_ } @{$result->{writeErrors}};
}

sub _next {
  my ($self, $group, $full, $cb) = @_;

  my ($type, $offset, $command) = $self->_group($group);
  $self->collection->db->command(
    $command => sub {
      my ($db, $err, $result) = @_;

      _merge($type, $offset, $full, $result);
      $err ||= $self->collection->db->mango->protocol->write_error($full);
      return $self->$cb($err, $full) if $err;

      return $self->$cb(undef, $full) unless my $next = shift @{$self->{ops}};
      $self->_next($next, $full, $cb);
    }
  );
}

sub _op {
  my ($self, $type, $doc) = @_;

  my $mango     = $self->collection->db->mango;
  my $bson_max  = $mango->max_bson_size;
  my $batch_max = $mango->max_write_batch_size;
  my $ops       = $self->{ops} ||= [];
  my $previous  = @$ops ? $ops->[-1] : [];
  my $bson      = bson_encode $doc;
  my $size      = length $bson;
  my $new       = ($self->{size} // 0) + $size;
  my $limit     = $new > $bson_max || @$previous >= $batch_max + 2;

  # Pre-encode documents and group them based on type and size
  push @$ops, [$type, $self->{offset} || 0] and delete $self->{size}
    if !@$previous || $previous->[0] ne $type || $limit;
  push @{$ops->[-1]}, bson_raw $bson;
  $self->{size} += $size;
  $self->{offset}++;

  return $self;
}

sub _remove {
  my ($self, $limit) = @_;
  my $query = delete $self->{query} // {};
  return $self->_op(delete => {q => $query, limit => $limit});
}

sub _set {
  my ($self, $key, $value) = @_;
  $self->{$key} = $value;
  return $self;
}

sub _update {
  my ($self, $multi, $update) = @_;
  my $query = delete $self->{query} // {};
  my $upsert = delete $self->{upsert} ? \1 : \0;
  return $self->_op(
    update => {q => $query, u => $update, multi => $multi, upsert => $upsert});
}

1;

=encoding utf8

=head1 NAME

Mango::Bulk - MongoDB bulk operations

=head1 SYNOPSIS

  use Mango::Bulk;

  my $bulk = Mango::Bulk->new(collection => $collection);
  $bulk->insert({foo => 'bar'})->insert({foo => 'baz'})->execute;

=head1 DESCRIPTION

L<Mango::Bulk> is a container for MongoDB bulk operations, all operations will
be automatically grouped so they don't exceed L<Mango/"max_bson_size">.

=head1 ATTRIBUTES

L<Mango::Bulk> implements the following attributes.

=head2 collection

  my $collection = $bulk->collection;
  $bulk          = $bulk->collection(Mango::Collection->new);

L<Mango::Collection> object this bulk operation belongs to.

=head2 ordered

  my $ordered = $bulk->ordered;
  $bulk       = $bulk->ordered(1);

Bulk operations are ordered, defaults to C<1>.

=head1 METHODS

L<Mango::Bulk> inherits all methods from L<Mojo::Base> and implements the
following new ones.

=head2 execute

  my $results = $bulk->execute;

Execute bulk operations. You can also append a callback to perform operation
non-blocking.

  $bulk->execute(sub {
    my ($bulk, $err, $results) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 find

  $bulk = $bulk->find({foo => 'bar'});

Query for next update or remove operation.

=head2 insert

  $bulk = $bulk->insert({foo => 'bar'});

Insert document.

=head2 remove

  $bulk = $bulk->remove;

Remove multiple documents.

=head2 remove_one

  $bulk = $bulk->remove_one;

Remove one document.

=head2 update

  $bulk = $bulk->update({foo => 'bar'});

Update multiple documents.

=head2 update_one

  $bulk = $bulk->update_one({foo => 'baz'});

Update one document.

=head2 upsert

  $bulk = $bulk->upsert;

Next update operation will be an C<upsert>.

=head1 SEE ALSO

L<Mango>, L<Mojolicious::Guides>, L<http://mojolicio.us>.

=cut
