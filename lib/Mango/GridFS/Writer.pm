package Mango::GridFS::Writer;
use Mojo::Base -base;

use Carp 'croak';
use List::Util 'first';
use Mango::BSON qw(bson_bin bson_doc bson_oid bson_time);
use Mojo::IOLoop;

has chunk_size => 261120;
has [qw(content_type filename gridfs metadata)];

sub close {
  my ($self, $cb) = @_;

  # Already closed
  if ($self->{closed}++) {
    my $files_id = $self->_files_id;
    return $files_id unless $cb;
    return Mojo::IOLoop->next_tick(sub { $self->$cb(undef, $files_id) });
  }

  my @index   = (bson_doc(files_id => 1, n => 1), {unique => \1});
  my $gridfs  = $self->gridfs;
  my $command = bson_doc filemd5 => $self->_files_id, root => $gridfs->prefix;

  # Non-blocking
  my $chunks = $gridfs->chunks;
  my $bulk   = $chunks->bulk;
  my $files  = $gridfs->files;
  return Mojo::IOLoop->delay(
    sub { $self->_chunk($bulk)->execute(shift->begin) },
    sub {
      my ($delay, $err) = @_;
      return $delay->pass($err) if $err;
      $files->ensure_index({filename => 1} => $delay->begin);
      $chunks->ensure_index(@index => $delay->begin);
    },
    sub {
      my ($delay, $files_err, $chunks_err) = @_;
      if (my $err = $files_err || $chunks_err) { return $delay->pass($err) }
      $gridfs->db->command($command => $delay->begin);
    },
    sub {
      my ($delay, $err, $doc) = @_;
      return $delay->pass($err) if $err;
      $files->insert($self->_meta($doc->{md5}) => $delay->begin);
    },
    sub { shift; $self->$cb(shift, $self->_files_id) }
  ) if $cb;

  # Blocking
  $self->_chunk($bulk)->execute;
  $files->ensure_index({filename => 1});
  $chunks->ensure_index(@index);
  my $md5 = $gridfs->db->command($command)->{md5};
  $files->insert($self->_meta($md5));
  return $self->_files_id;
}

sub is_closed { !!shift->{closed} }

sub write {
  my ($self, $chunk, $cb) = @_;

  # Already closed
  if ($self->is_closed) {
    croak 'File already closed' unless $cb;
    return Mojo::IOLoop->next_tick(sub { $self->$cb('File already closed') });
  }

  $self->{buffer} .= $chunk;
  $self->{len} += length $chunk;

  my $bulk = $self->gridfs->chunks->bulk->ordered(0);
  my $size = $self->chunk_size;
  $self->_chunk($bulk) while length $self->{buffer} >= $size;

  # Non-blocking
  return $bulk->execute(sub { shift; $self->$cb(shift) }) if $cb;

  # Blocking
  $bulk->execute;
  return $self;
}

sub _chunk {
  my ($self, $bulk) = @_;

  my $chunk = substr $self->{buffer}, 0, $self->chunk_size, '';
  return $bulk unless length $chunk;

  my $n = $self->{n}++;
  return $bulk->insert(
    {files_id => $self->_files_id, n => $n, data => bson_bin($chunk)});
}

sub _files_id { shift->{files_id} //= bson_oid }

sub _meta {
  my ($self, $md5) = @_;

  my $doc = {
    _id        => $self->_files_id,
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

Chunk size in bytes, defaults to C<261120> (255KB).

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

=head2 is_closed

  my $success = $writer->is_closed;

Check if file has been closed.

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
