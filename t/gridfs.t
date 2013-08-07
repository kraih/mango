use Mojo::Base -strict;

use Test::More;

plan skip_all => 'set TEST_ONLINE to enable this test'
  unless $ENV{TEST_ONLINE};

use Mango;
use Mango::BSON 'bson_oid';
use Mojo::IOLoop;

# Clean up before start
my $mango  = Mango->new($ENV{TEST_ONLINE});
my $gridfs = $mango->db->gridfs;
$gridfs->$_->remove for qw(files chunks);

# Blocking roundtrip
my $writer = $gridfs->writer;
$writer->filename('foo.txt')->content_type('text/plain')
  ->metadata({foo => 'bar'});
ok !$writer->is_closed, 'file has not been closed';
my $oid = $writer->write('hello ')->write('world!')->close;
ok $writer->is_closed, 'file has been closed';
my $reader = $gridfs->reader;
is $reader->tell, 0, 'right position';
$reader->open($oid);
is $reader->filename,     'foo.txt',    'right filename';
is $reader->content_type, 'text/plain', 'right content type';
is_deeply $reader->metadata, {foo => 'bar'}, 'right structure';
is $reader->size,       12,     'right size';
is $reader->chunk_size, 262144, 'right chunk size';
is length $reader->upload_date, length(time) + 3, 'right time format';
my $data;
while (defined(my $chunk = $reader->read)) { $data .= $chunk }
is $reader->tell, 12, 'right position';
is $data, 'hello world!', 'right content';
$data = undef;
$reader->seek(0);
is $reader->tell, 0, 'right position';
$reader->seek(2);
is $reader->tell, 2, 'right position';
while (defined(my $chunk = $reader->read)) { $data .= $chunk }
is $data, 'llo world!', 'right content';
is_deeply $gridfs->list, ['foo.txt'], 'right files';
$gridfs->delete($oid);
is_deeply $gridfs->list, [], 'no files';
is $gridfs->chunks->find->count, 0, 'no chunks left';
$gridfs->$_->drop for qw(files chunks);

# Non-blocking roundtrip
$writer = $gridfs->writer->chunk_size(4);
$writer->filename('foo.txt')->content_type('text/plain')
  ->metadata({foo => 'bar'});
ok !$writer->is_closed, 'file has not been closed';
my ($fail, $result);
my $delay = Mojo::IOLoop->delay(
  sub {
    my $delay = shift;
    $writer->write('he' => $delay->begin);
  },
  sub {
    my ($delay, $err) = @_;
    $fail = $err;
    $writer->write('llo ' => $delay->begin);
  },
  sub {
    my ($delay, $err) = @_;
    $fail ||= $err;
    $writer->write('w'     => $delay->begin);
    $writer->write('orld!' => $delay->begin);
  },
  sub {
    my ($delay, $err) = @_;
    $fail ||= $err;
    $writer->close($delay->begin);
  },
  sub {
    my ($delay, $err, $oid) = @_;
    $fail ||= $err;
    $result = $oid;
  }
);
$delay->wait;
ok !$fail, 'no error';
ok $writer->is_closed, 'file has been closed';
$reader = $gridfs->reader;
$fail   = undef;
$reader->open(
  $result => sub {
    my ($reader, $err) = @_;
    $fail = $err;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
ok !$fail, 'no error';
is $reader->filename,     'foo.txt',    'right filename';
is $reader->content_type, 'text/plain', 'right content type';
is_deeply $reader->metadata, {foo => 'bar'}, 'right structure';
is $reader->size,       12, 'right size';
is $reader->chunk_size, 4,  'right chunk size';
is length $reader->upload_date, length(time) + 3, 'right time format';
($fail, $data) = ();
my $cb;
$cb = sub {
  my ($reader, $err, $chunk) = @_;
  $fail ||= $err;
  return Mojo::IOLoop->stop unless defined $chunk;
  $data .= $chunk;
  $reader->read($cb);
};
$reader->$cb(undef, '');
Mojo::IOLoop->start;
ok !$fail, 'no error';
is $data, 'hello world!', 'right content';
my ($before, $after);
$fail  = undef;
$delay = Mojo::IOLoop->delay(
  sub { $gridfs->list(shift->begin) },
  sub {
    my ($delay, $err, $names) = @_;
    $fail   = $err;
    $before = $names;
    $gridfs->delete($result => $delay->begin);
  },
  sub {
    my ($delay, $err) = @_;
    $fail ||= $err;
    $gridfs->list($delay->begin);
  },
  sub {
    my ($delay, $err, $names) = @_;
    $fail ||= $err;
    $after = $names;
  }
);
$delay->wait;
ok !$fail, 'no error';
is_deeply $before, ['foo.txt'], 'right files';
is_deeply $after, [], 'no files';
is $gridfs->chunks->find->count, 0, 'no chunks left';
$gridfs->$_->drop for qw(files chunks);

# Find and slurp versions blocking
my $one
  = $gridfs->writer->chunk_size(1)->filename('test.txt')->write('One')->close;
my $two = $gridfs->writer->filename('test.txt')->write('Two')->close;
is_deeply $gridfs->list, ['test.txt'], 'right files';
is $gridfs->find_version('test.txt', 1), $one, 'right version';
is $gridfs->find_version('test.txt', 2), $two, 'right version';
is $gridfs->find_version('test.txt', 3), undef, 'no version';
is $gridfs->reader->open($one)->slurp, 'One', 'right content';
is $gridfs->reader->open($one)->seek(1)->slurp, 'ne', 'right content';
is $gridfs->reader->open($two)->slurp, 'Two', 'right content';
is $gridfs->reader->open($two)->seek(1)->slurp, 'wo', 'right content';
$gridfs->$_->drop for qw(files chunks);

# Find and slurp versions non-blocking
$one = $gridfs->writer->filename('test.txt')->write('One')->close;
$two = $gridfs->writer->filename('test.txt')->write('Two')->close;
is_deeply $gridfs->list, ['test.txt'], 'right files';
my @results;
$fail  = undef;
$delay = Mojo::IOLoop->delay(
  sub {
    my $delay = shift;
    $gridfs->find_version(('test.txt', 3) => $delay->begin);
    $gridfs->find_version(('test.txt', 2) => $delay->begin);
    $gridfs->find_version(('test.txt', 1) => $delay->begin);
  },
  sub {
    my ($delay, $three_err, $three, $two_err, $two, $one_err, $one) = @_;
    $fail = $one_err || $two_err || $three_err;
    @results = ($one, $two, $three);
  }
);
$delay->wait;
ok !$fail, 'no error';
is $results[0], $one, 'right version';
is $results[1], $two, 'right version';
is $results[2], undef, 'no version';
my $reader_one = $gridfs->reader;
my $reader_two = $gridfs->reader;
($fail, @results) = ();
$delay = Mojo::IOLoop->delay(
  sub {
    my $delay = shift;
    $reader_one->open($one => $delay->begin);
    $reader_two->open($two => $delay->begin);
  },
  sub {
    my ($delay, $one_err, $two_err) = @_;
    $fail = $one_err || $two_err;
    $reader_one->slurp($delay->begin);
    $reader_two->slurp($delay->begin);
  },
  sub {
    my ($delay, $one_err, $one, $two_err, $two) = @_;
    $fail ||= $one_err || $two_err;
    @results = ($one, $two);
  }
);
$delay->wait;
ok !$fail, 'no error';
is $results[0], 'One', 'right content';
is $results[1], 'Two', 'right content';
$gridfs->$_->drop for qw(files chunks);

# File already closed
$writer = $gridfs->writer;
ok !$writer->is_closed, 'file has not been closed';
$oid = $writer->write('Test')->close;
ok $writer->is_closed, 'file has been closed';
eval { $writer->write('123') };
like $@, qr/^File already closed/, 'right error';
$fail = undef;
$writer->write(
  '123' => sub {
    my ($writer, $err) = @_;
    $fail = $err;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
like $fail, qr/^File already closed/, 'right error';
ok $writer->is_closed, 'file is still closed';
is $writer->close, $oid, 'right result';
($fail, $result) = ();
$writer->close(
  sub {
    my ($writer, $err, $oid) = @_;
    $fail   = $err;
    $result = $oid;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
ok !$fail, 'no error';
is $result, $oid, 'right result';
ok $writer->is_closed, 'file is still closed';
$gridfs->$_->drop for qw(files chunks);

# Missing file
is $gridfs->reader->open(bson_oid)->slurp, undef, 'no content';

done_testing();
