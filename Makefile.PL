use 5.010001;

use strict;
use warnings;

use Config;
use ExtUtils::MakeMaker;

die "Perl with support for quads is required!\n"
  unless (($Config{use64bitint} // '') eq 'define' || $Config{longsize} >= 8);

WriteMakefile(
  NAME         => 'Mango',
  VERSION_FROM => 'lib/Mango.pm',
  ABSTRACT     => 'Pure-Perl non-blocking I/O MongoDB client',
  AUTHOR       => 'Sebastian Riedel <sri@cpan.org>',
  LICENSE      => 'artistic_2',
  META_MERGE   => {
    requires  => {perl => '5.010001'},
    resources => {
      homepage    => 'http://mojolicio.us',
      license     => 'http://www.opensource.org/licenses/artistic-license-2.0',
      MailingList => 'http://groups.google.com/group/mojolicious',
      repository  => 'http://github.com/kraih/mango',
      bugtracker  => 'http://github.com/kraih/mango/issues'
    },
    no_index => {directory => ['t']}
  },
  PREREQ_PM => {Mojolicious => '4.0'},
  test      => {TESTS       => 't/*.t t/*/*.t'}
);
