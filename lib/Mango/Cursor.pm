package Mango::Cursor;
use Mojo::Base -base;

use Mojo::IOLoop;

has [qw(collection id)];
has [qw(batch_size limit)] => 0;

sub add_batch {
  my ($self, $docs) = @_;
  push @{$self->{results} ||= []}, @$docs;
  return $self;
}

sub all {
  my ($self, $cb) = @_;

  # Non-blocking
  my @all;
  return $self->next(sub { shift->_collect(\@all, $cb, @_) }) if $cb;

  # Blocking
  while (my $next = $self->next) { push @all, $next }
  return \@all;
}

sub next {
  my ($self, $cb) = @_;
  return defined $self->id ? $self->_continue($cb) : $self->_start($cb);
}

sub num_to_return {
  my $self  = shift;
  my $limit = $self->limit;
  my $size  = $self->batch_size;
  return $limit == 0 || ($size > 0 && $size < $limit) ? $size : $limit;
}

sub rewind {
  my ($self, $cb) = @_;

  delete @$self{qw(num results)};
  return $cb ? $self->_defer($cb) : undef unless defined(my $id = $self->id);
  $self->id(undef);

  # Non-blocking
  my $mango = $self->collection->db->mango;
  return $mango->kill_cursors($id => sub { shift; $self->$cb(@_) }) if $cb;

  # Blocking
  $mango->kill_cursors($id);
}

sub _collect {
  my ($self, $all, $cb, $err, $doc) = @_;
  return $self->_defer($cb, $err, $all) if $err || !$doc;
  push @$all, $doc;
  $self->next(sub { shift->_collect($all, $cb, @_) });
}

sub _continue {
  my ($self, $cb) = @_;

  my $collection = $self->collection;
  my $name       = $collection->full_name;
  my $mango      = $collection->db->mango;

  # Non-blocking
  if ($cb) {
    return $self->_defer($cb, undef, $self->_dequeue) if $self->_enough;
    return $mango->get_more(($name, $self->num_to_return, $self->id) =>
        sub { shift; $self->$cb(shift, $self->_enqueue(shift)) });
  }

  # Blocking
  return $self->_dequeue if $self->_enough;
  return $self->_enqueue(
    $mango->get_more($name, $self->num_to_return, $self->id));
}

sub _defer {
  my ($self, $cb, @args) = @_;
  Mojo::IOLoop->next_tick(sub { $self->$cb(@args) });
}

sub _dequeue {
  my $self = shift;
  return undef if $self->_finished;
  $self->{num}++;
  return shift @{$self->{results}};
}

sub _enough {
  my $self = shift;
  return $self->id eq '0' || $self->_finished || !!@{$self->{results} // []};
}

sub _enqueue {
  my ($self, $reply) = @_;
  return undef unless $reply;
  return $self->add_batch($reply->{docs})->id($reply->{cursor})->_dequeue;
}

sub _finished {
  my $self = shift;
  return undef unless my $limit = $self->limit;
  return ($self->{num} // 0) >= abs($limit) ? 1 : undef;
}

sub _start { die 'Cursor cannot be restarted' }

1;

=encoding utf8

=head1 NAME

Mango::Cursor - MongoDB cursor

=head1 SYNOPSIS

  use Mango::Cursor;

  my $cursor = Mango::Cursor->new(collection => $collection);
  my $docs   = $cursor->all;

=head1 DESCRIPTION

L<Mango::Cursor> is a container for MongoDB cursors used by
L<Mango::Collection>.

=head1 ATTRIBUTES

L<Mango::Cursor> implements the following attributes.

=head2 batch_size

  my $size = $cursor->batch_size;
  $cursor  = $cursor->batch_size(10);

Number of documents to fetch in one batch, defaults to C<0>.

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

Limit the number of documents, defaults to C<0>.

=head1 METHODS

L<Mango::Cursor> inherits all methods from L<Mojo::Base> and implements the
following new ones.

=head2 add_batch

  $cursor = $cursor->add_batch($docs);

Add batch of documents to cursor.

=head2 all

  my $docs = $cursor->all;

Fetch all documents at once. You can also append a callback to perform
operation non-blocking.

  $cursor->all(sub {
    my ($cursor, $err, $docs) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 next

  my $doc = $cursor->next;

Fetch next document. You can also append a callback to perform operation
non-blocking.

  $cursor->next(sub {
    my ($cursor, $err, $doc) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 rewind

  $cursor->rewind;

Rewind cursor and kill it on the server. You can also append a callback to
perform operation non-blocking.

  $cursor->rewind(sub {
    my ($cursor, $err) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 num_to_return

  my $num = $cursor->num_to_return;

Number of results to return with next C<QUERY> or C<GET_MORE> operation based
on L</"batch_size"> and L</"limit">.

=head1 SEE ALSO

L<Mango>, L<Mojolicious::Guides>, L<http://mojolicio.us>.

=cut
