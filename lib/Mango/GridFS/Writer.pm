package Mango::GridFS::Writer;
use Mojo::Base -base;

use List::Util 'first';
use Mango::BSON qw(bson_bin bson_doc bson_oid bson_time bson_true);
use Mojo::IOLoop;

has chunk_size => 262144;
has [qw(content_type filename gridfs)];

sub close {
  my $self = shift;

  $self->_chunk;

  my $gridfs = $self->gridfs;
  my $files  = $gridfs->files;
  $files->ensure_index({filename => 1});
  $gridfs->chunks->ensure_index(bson_doc(files_id => 1, n => 1),
    {unique => bson_true});

  my $command = bson_doc
    filemd5 => $self->{files_id},
    root    => $gridfs->prefix;
  my $md5 = $gridfs->db->command($command)->{md5};

  my $doc = {
    _id        => $self->{files_id},
    length     => $self->{len},
    chunkSize  => $self->chunk_size,
    uploadDate => bson_time,
    md5        => $md5
  };
  if (my $name = $self->filename)     { $doc->{filename}    = $name }
  if (my $type = $self->content_type) { $doc->{contentType} = $type }
  $files->insert($doc);

  return $self->{files_id};
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

=head1 METHODS

L<Mango::GridFS::Writer> inherits all methods from L<Mojo::Base> and
implements the following new ones.

=head2 close

  my $oid = $writer->close;

Close file.

=head2 write

  $writer->write('hello world!');

Write chunk. You can also append a callback to perform operation non-blocking.

  $writer->write('hello world!' => sub {
    my ($writer, $err) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head1 SEE ALSO

L<Mango>, L<Mojolicious::Guides>, L<http://mojolicio.us>.

=cut
