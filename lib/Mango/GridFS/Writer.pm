package Mango::GridFS::Writer;
use Mojo::Base -base;

use List::Util 'first';
use Mango::BSON qw(bson_bin bson_doc bson_oid bson_time bson_true);
use Mojo::IOLoop;

has chunk_size => 262144;
has [qw(content_type filename gridfs metadata)];

sub close {
  my ($self, $cb) = @_;

  my @index   = (bson_doc(files_id => 1, n => 1), {unique => bson_true});
  my $gridfs  = $self->gridfs;
  my $command = bson_doc
    filemd5 => $self->{files_id},
    root    => $gridfs->prefix;

  # Blocking
  my $files = $gridfs->files;
  unless ($cb) {
    $self->_chunk;
    $files->ensure_index({filename => 1});
    $gridfs->chunks->ensure_index(@index);
    my $md5 = $gridfs->db->command($command)->{md5};
    $files->insert($self->_meta($md5));
    return $self->{files_id};
  }

  # Non-blocking
  Mojo::IOLoop->delay(
    sub { $self->_chunk(shift->begin) },
    sub {
      my ($delay, $err) = @_;
      return $self->$cb($err) if $err;
      $files->ensure_index({filename => 1} => $delay->begin);
      $gridfs->chunks->ensure_index(@index => $delay->begin);
    },
    sub {
      my ($delay, $files_err, $chunks_err) = @_;
      if (my $err = $files_err || $chunks_err) { return $self->$cb($err) }
      $gridfs->db->command($command => $delay->begin);
    },
    sub {
      my ($delay, $err, $doc) = @_;
      return $self->$cb($err) if $err;
      $files->insert($self->_meta($doc->{md5}) => $delay->begin);
    },
    sub {
      my ($delay, $err) = @_;
      $self->$cb($err, $self->{files_id});
    }
  );
}

sub write {
  my ($self, $chunk, $cb) = @_;

  $self->{buffer} .= $chunk;
  $self->{len} += length $chunk;

  # Non-blocking
  my $size = $self->chunk_size;
  if ($cb) {
    my $delay = Mojo::IOLoop->delay(sub { shift; $self->_err($cb, @_) });
    $self->_chunk($delay->begin) while length $self->{buffer} >= $size;
    $delay->begin->(undef, undef);
  }

  # Blocking
  else { $self->_chunk while length $self->{buffer} >= $size }

  return $self;
}

sub _chunk {
  my ($self, $cb) = @_;

  my $chunk = substr $self->{buffer}, 0, $self->chunk_size, '';
  return $cb ? Mojo::IOLoop->timer(0 => $cb) : () unless length $chunk;

  # Blocking
  my $n   = $self->{n}++;
  my $oid = $self->{files_id} //= bson_oid;
  my $doc = {files_id => $oid, n => $n, data => bson_bin($chunk)};
  return $self->gridfs->chunks->insert($doc) unless $cb;

  # Non-blocking
  $self->gridfs->chunks->insert($doc => $cb);
}

sub _err {
  my ($self, $cb) = (shift, shift);
  $self->$cb(first {defined} @_[map { 2 * $_ } 0 .. @_ / 2]);
}

sub _meta {
  my ($self, $md5) = @_;

  my $doc = {
    _id        => $self->{files_id},
    length     => $self->{len},
    chunkSize  => $self->chunk_size,
    uploadDate => bson_time,
    md5        => $md5
  };
  if (my $name = $self->filename)     { $doc->{filename}    = $name }
  if (my $type = $self->content_type) { $doc->{contentType} = $type }
  if (my $data = $self->metadata)     { $doc->{metadata}    = $data }

  return $doc;
}

1;

=encoding utf8

=head1 NAME

Mango::GridFS::Writer - GridFS writer

=head1 SYNOPSIS

  use Mango::GridFS::Writer;

  my $writer = Mango::GridFS::Writer->new(gridfs => $gridfs);

=head1 DESCRIPTION

L<Mango::GridFS::Writer> writes files to GridFS.

=head1 ATTRIBUTES

L<Mango::GridFS::Writer> implements the following attributes.

=head2 chunk_size

  my $size = $writer->chunk_size;
  $writer  = $writer->chunk_size(1024);

Chunk size in bytes, defaults to C<262144>.

=head2 content_type

  my $type = $writer->content_type;
  $writer  = $writer->content_type('text/plain');

Content type of file.

=head2 filename

  my $name = $writer->filename;
  $writer  = $writer->filename('foo.txt');

Name of file.

=head2 gridfs

  my $gridfs = $writer->gridfs;
  $writer    = $writer->gridfs(Mango::GridFS->new);

L<Mango::GridFS> object this writer belongs to.

=head2 metadata

  my $data = $writer->metadata;
  $writer  = $writer->metadata({foo => 'bar'});

Additional information.

=head1 METHODS

L<Mango::GridFS::Writer> inherits all methods from L<Mojo::Base> and
implements the following new ones.

=head2 close

  my $oid = $writer->close;

Close file. You can also append a callback to perform operation non-blocking.

  $writer->close(sub {
    my ($writer, $err, $oid) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 write

  $writer = $writer->write('hello world!');

Write chunk. You can also append a callback to perform operation non-blocking.

  $writer->write('hello world!' => sub {
    my ($writer, $err) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head1 SEE ALSO

L<Mango>, L<Mojolicious::Guides>, L<http://mojolicio.us>.

=cut
