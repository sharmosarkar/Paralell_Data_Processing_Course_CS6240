#!/usr/bin/perl
use 5.16.0;
use warnings FATAL => 'all';

my $nodes = 5;

for (my $n_id = 0; $n_id < $nodes; ++$n_id) {
    my $amp = ($n_id == $nodes - 1) ? "" : "&";
    system("java -jar barrier.jar $n_id $nodes $amp");
}
