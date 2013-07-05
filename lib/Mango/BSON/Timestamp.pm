package Mango::BSON::Timestamp;
use Mojo::Base -base;

has [qw(seconds increment)];

1;

=encoding utf8

=head1 NAME

Mango::BSON::Timestamp - Timestamp type

=head1 SYNOPSIS

  use Mango::BSON::Timestamp;

  my $ts = Mango::BSON::Timestamp->new(seconds => 23, increment => 5);

=head1 DESCRIPTION

L<Mango::BSON::Timestamp> is a container for the BSON timestamp type used by
L<Mango::BSON>.

=head1 ATTRIBUTES

L<Mango::BSON::Timestamp> implements the following attributes.

=head2 seconds

  my $seconds = $ts->seconds;
  $ts         = $ts->seconds(23);

Seconds.

=head2 increment

  my $inc = $ts->increment;
  $tz     = $ts->increment(5);

Increment.

=head1 METHODS

L<Mango::BSON::Timestamp> inherits all methods from L<Mojo::Base>.

=head1 SEE ALSO

L<Mango>, L<Mojolicious::Guides>, L<http://mojolicio.us>.

=cut
