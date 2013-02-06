package Mango::BSON::Time;
use Mojo::Base -base;
use overload '""' => sub { ${$_[0]} }, fallback => 1;

use Time::HiRes 'time';

sub new {
  my ($class, $time) = @_;
  return bless \($time //= int(time * 1000)), ref $class || $class;
}

sub to_epoch { int(${$_[0]} / 1000) }

1;

=head1 NAME

Mango::BSON::Time - Datetime type

=head1 SYNOPSIS

  use Mango::BSON::Time;

  my $time = Mango::BSON::Time->new(time * 1000);
  say $time->to_epoch;

=head1 DESCRIPTION

L<Mango::BSON::Time> is a container for the BSON datetime type used by
L<Mango::BSON>.

=head1 METHODS

L<Mango::BSON::Time> inherits all methods from L<Mojo::Base> and implements
the following new ones.

=head2 new

  my $time = Mango::BSON::Time->new;
  my $time = Mango::BSON::Time->new(time * 1000);

Construct a new scalar-based L<Mango::BSON::Time> object.

=head2 to_epoch

  my $epoch = $time->to_epoch;

Convert time to epoch seconds.

=head1 SEE ALSO

L<Mango>, L<Mojolicious::Guides>, L<http://mojolicio.us>.

=cut
