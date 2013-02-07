package Mango::Cursor;
use Mojo::Base -base;

use Mango::BSON 'bson_doc';
use Mojo::IOLoop;

has [qw(batch_size limit skip)] => 0;
has [qw(collection id sort)];
has [qw(fields query)] => sub { {} };

sub all {
  my ($self, $cb) = @_;

  # Non-blocking
  my @all;
  return $self->next(sub { shift->_collect(\@all, $cb, @_) }) if $cb;

  # Blocking
  while (my $next = $self->next) { push @all, $next }
  return \@all;
}

sub count {
  my $self = shift;
  my $cb = ref $_[-1] eq 'CODE' ? pop : undef;

  my $collection = $self->collection;
  my $count      = bson_doc
    count => $collection->name,
    query => $self->_query,
    skip  => $self->skip,
    limit => $self->limit;

  # Non-blocking
  return $collection->db->command(
    $count => sub {
      my ($collection, $err, $doc) = @_;
      $self->$cb($err, $doc ? $doc->{n} : 0);
    }
  ) if $cb;

  # Blocking
  my $doc = $collection->db->command($count);
  return $doc ? $doc->{n} : 0;
}

sub next {
  my ($self, $cb) = @_;
  return exists $self->{results} ? $self->_continue($cb) : $self->_start($cb);
}

sub rewind {
  my ($self, $cb) = @_;

  delete $self->{$_} for qw(num results);
  return $cb ? $self->_defer($cb) : undef unless my $id = $self->id;
  $self->id(undef);

  # Non-blocking
  return $self->collection->db->mango->kill_cursors($id => sub { $self->$cb })
    if $cb;

  # Blocking
  $self->collection->db->mango->kill_cursors($id);
}

sub _collect {
  my ($self, $all, $cb, $err, $doc) = @_;
  return $self->_defer($cb, $err, $all) if $err || !$doc;
  push @$all, $doc;
  $self->next(sub { shift->_collect($all, $cb, @_) });
}

sub _continue {
  my ($self, $cb) = @_;

  # Non-blocking
  my $collection = $self->collection;
  my $name       = $collection->full_name;
  if ($cb) {
    return $self->_defer($cb, undef, $self->_dequeue) if $self->_enough;
    return $collection->db->mango->get_more(
      ($name, $self->_max, $self->id) => sub {
        my ($mango, $err, $reply) = @_;
        $self->$cb($err, $self->_enqueue($reply));
      }
    );
  }

  # Blocking
  return $self->_dequeue if $self->_enough;
  return $self->_enqueue(
    $collection->db->mango->get_more($name, $self->_max, $self->id));
}

sub _defer {
  my ($self, $cb, @args) = @_;
  Mojo::IOLoop->timer(0 => sub { $self->$cb(@args) });
}

sub _dequeue {
  my $self = shift;
  return undef if $self->_finished;
  $self->{num}++;
  return shift @{$self->{results}};
}

sub _enough { $_[0]->_finished ? 1 : !!@{$_[0]->{results}} }

sub _finished {
  my $self = shift;
  return undef unless my $limit = $self->limit;
  $limit = $limit * -1 if $limit < 0;
  return ($self->{num} // 0) >= $limit ? 1 : undef;
}

sub _enqueue {
  my ($self, $reply) = @_;
  return unless $reply;
  push @{$self->{results} ||= []}, @{$reply->{docs}};
  return $self->_dequeue;
}

sub _max {
  my $self  = shift;
  my $limit = $self->limit;
  my $size  = $self->batch_size;
  return $size if $limit == 0;
  return $size > $limit ? $limit : $size;
}

sub _query {
  my $self  = shift;
  my $query = $self->query;
  return $query unless my $sort = $self->sort;
  return {'$query' => $query, '$orderby' => $sort};
}

sub _start {
  my ($self, $cb) = @_;

  my $collection = $self->collection;
  my $name       = $collection->full_name;
  my @args
    = ($name, {}, $self->skip, $self->_max, $self->_query, $self->fields);

  # Non-blocking
  return $collection->db->mango->query(
    @args => sub {
      my ($mango, $err, $reply) = @_;
      $self->id($reply->{cursor}) if $reply;
      $self->$cb($err, $self->_enqueue($reply));
    }
  ) if $cb;

  # Blocking
  my $reply = $collection->db->mango->query(@args);
  $self->id($reply->{cursor}) if $reply;
  return $self->_enqueue($reply);
}

1;

=head1 NAME

Mango::Cursor - MongoDB cursor

=head1 SYNOPSIS

  use Mango::Cursor;

  my $cursor = Mango::Cursor->new(collection => $collection);

=head1 DESCRIPTION

L<Mango::Cursor> is a container for MongoDB cursors used by
L<Mango::Collection>.

=head1 ATTRIBUTES

L<Mango::Cursor> implements the following attributes.

=head2 batch_size

  my $size = $cursor->batch_size;
  $cursor  = $cursor->batch_size(10);

Batch size, defaults to C<0>.

=head2 collection

  my $collection = $cursor->collection;
  $cursor        = $cursor->collection(Mango::Collection->new);

L<Mango::Collection> object this cursor belongs to.

=head2 id

  my $id  = $cursor->id;
  $cursor = $cursor->id(123456);

Cursor id.

=head2 limit

  my $limit = $cursor->limit;
  $cursor   = $cursor->limit(10);

Limit, defaults to C<0>.

=head2 fields

  my $fields = $cursor->fields;
  $cursor    = $cursor->fields({foo => 1});

Fields.

=head2 query

  my $query = $cursor->query;
  $cursor   = $cursor->query({foo => 'bar'});

Query.

=head2 skip

  my $skip = $cursor->skip;
  $cursor  = $cursor->skip(5);

Documents to skip, defaults to C<0>.

=head2 sort

  my $sort = $cursor->sort;
  $cursor  = $cursor->sort({foo => 1});

Sort.

=head1 METHODS

L<Mango::Cursor> inherits all methods from L<Mojo::Base> and implements the
following new ones.

=head2 all

  my $docs = $cursor->all;

Fetch all documents. You can also append a callback to perform operation
non-blocking.

  $cursor->all(sub {
    my ($cursor, $err, $docs) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 count

  my $count = $cursor->count;

Count number of documents this cursor can return. You can also append a
callback to perform operation non-blocking.

  $cursor->count(sub {
    my ($cursor, $err, $count) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 next

  my $doc  = $cursor->next;

Fetch next document. You can also append a callback to perform operation
non-blocking.

  $cursor->next(sub {
    my ($cursor, $err, $doc) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 rewind

  $cursor->rewind;

Rewind cursor. You can also append a callback to perform operation
non-blocking.

  $cursor->rewind(sub {
    my $cursor = shift;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head1 SEE ALSO

L<Mango>, L<Mojolicious::Guides>, L<http://mojolicio.us>.

=cut
