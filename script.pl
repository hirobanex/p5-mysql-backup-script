#!/usr/bin/env perl
use strict;
use warnings;
use utf8;
use Net::OpenSSH;
use Time::Piece;
use Time::Seconds;
use Smart::Options::Declare;
use Smart::Args;
use Path::Class;
use Encode;
use Email::Sender::Simple qw/sendmail/;
use Email::Simple;
use Email::Simple::Creator;
use Data::Recursive::Encode;
use Email::Sender::Transport::SMTP;
use Log::Minimal;
use File::RotateLogs;
use File::Copy::Recursive;

#you must setup .ssh/config remote server setting
#ex. host hirobanex.net(user,ip,port)

opts_coerce Method => 'Str', sub {
    $_[0] or return;

    (($_[0] eq 'bin_log') || ($_[0] eq 'mysqldump')) 
        or die 'Method name missing !';
    $_[0];
};

opts my $init   => 'Bool';
opts my $method => 'Method';
opts my $target => 'Str';

my $rotatelogs = File::RotateLogs->new(
    logfile      =>  './log/exec_log.%Y%m%d%H%M%S',
    linkname     => './log/exec_log',
    rotationtime => 86400 * 14, #2week
    maxage       => 86400 * 30 * 12, #1year
);

local $Log::Minimal::PRINT = sub {
   my ( $time, $type, $message, $trace,$raw_message) = @_;
   $rotatelogs->print("$time [$type] $message at $trace\n");
};

my $t            = localtime;
my $config       = do { 
    my $global = do './config.pl';
    my $local  = do './config_local.pl';

    +{%$global,%$local};
};
my $remote_user  = $config->{remote_user};
my $base_log_dir = Path::Class::dir('backup_data');

=for config
+{ 
    hosts => +[qw/
        host_1 host_2 host_3
    /],
}
=cut

($method && $target) 
    ? main->$method($target,$base_log_dir->subdir($target))
    : main()
;

sub main {
    my $start_time = time;
    infof('start');

    for my $host ( @{$config->{hosts}} ) {
        my $log_dir = $base_log_dir->subdir($host);
        
        (-d $log_dir) or do {
            $log_dir->mkpath;
            infof('make dir:%s',$log_dir);
        };
        
        ( $init || ($t->fullday eq 'Monday')) 
            ? mysqldump(undef,$host,$log_dir)
            : bin_log(undef,$host,$log_dir)
        ;
    }

    my $exec_time = time - $start_time;
    infof('end.exec_time: %s', $exec_time);
    report($exec_time);
}

sub report {
    my ($exec_time) = @_;

    my $email = Email::Simple->create(
        header => Data::Recursive::Encode->encode(
            'MIME-Header-ISO_2022_JP' => [
                To                          => $config->{mail}->{To},
                From                        => $config->{mail}->{From},
                Subject                     => $config->{mail}->{Subject},
                'Content-Type'              => 'text/plain; charset=ISO-2022-JP',
                'Content-Transfer-Encoding' => '7bit',
            ]
        ),
        body       => encode( 'iso-2022-jp', "本日のバックアップ時間は${exec_time}秒でした。"),
        attributes => {
            content_type => 'text/plain',
            charset      => 'ISO-2022-JP',
            encoding     => '7bit',
        },
    );
    my $sender = Email::Sender::Transport::SMTP->new($config->{smtp});

    sendmail($email, {transport => $sender});
}

sub bin_log {#main->bin_logってやっているから$selfをとる・・・
    my ($self,$host,$log_dir) = @_;

    my $start_time = time;
    infof('[%s] start bin_log-method',$host);

    my $ssh = get_ssh_instance($host);

    ssh_exec($host,$ssh,'mysqladmin -uroot flush-logs');

    ssh_exec($host,$ssh,'rm -rf mysql-bin && mkdir mysql-bin');

    my @out = split "\n", ssh_exec($host,$ssh,'sudo ls /var/lib/mysql/ | grep mysql-bin');

    for my $out (@out) {
        ssh_exec($host,$ssh,"sudo cp /var/lib/mysql/$out /home/$remote_user/mysql-bin/");
    }

    ssh_exec($host,$ssh,"sudo chown -R $remote_user:$remote_user ./mysql-bin/ && sudo chmod 666 ./mysql-bin/* && gzip ./mysql-bin/*");

    infof("[%s] copy mysql-bin files to /home/$remote_user/mysql-bin/ end",$host);

    $ssh->rsync_get(
        +{
            exclude    => '*~',
            safe_links => 1,
            archive    => 1,
            compress   => 1,
            delete     => 1,
        },
        "/home/$remote_user/mysql-bin",
        $log_dir->stringify
    );
    infof('[%s] rsync end',$host);

    my $exec_time = time - $start_time;
    infof('[%s] bin_log-method time:%s',$host,$exec_time);
}

sub mysqldump {#main->mysqldumpってやっているから$selfをとる・・・
    my ($self,$host,$log_dir) = @_;
    
    my $start_time = time;
    infof('[%s] start mysqldump-method',$host);

    my $before_month = $t - ONE_MONTH;
    for my $file ($log_dir->children) {
        next if $file !~ /^masqldump\.gz/;

        if ( $before_month > localtime($file->stat->mtime)){
            $file->remove or croakf("[%s]  %s remove failed : %s",$host,$file->stringify,$!);
            infof('[%s] removed %s',$host,$file->stringify);
        }
    }

    if (-f $log_dir->file('mysqldump.gz')) {
        my $before_week = $t - ONE_WEEK;

         File::Copy::Recursive::fmove($log_dir->file('mysqldump.gz'),$log_dir->file('mysqldump.gz.'.$before_week->strftime('%Y%m%d'))) 
            or croakf("[%s] mysqldump.gz fmove failed: %s",$host,$!);
    }
    infof('[%s] pre-task end',$host);

    my $ssh = get_ssh_instance($host);

    ssh_exec($host,$ssh,"mysqldump --user=root --events --all-databases --opt --flush-logs --single-transaction --master-data=2 | gzip > ./mysqldump.gz");
    infof('[%s] mysqldump end',$host);

    $ssh->rsync_get(+{}, "/home/$remote_user/mysqldump.gz",$log_dir->stringify);
    
    infof('[%s] rsync_get end',$host);

    my $exec_time = time - $start_time;
    infof('[%s] mysqldump-method time : %s',$host,$exec_time);
}

sub get_ssh_instance { Net::OpenSSH->new($_[0]) }
sub ssh_exec {
    my ($host,$ssh,@cmd) = @_;

    my ($out, $err) = $ssh->capture2(@cmd);

    $ssh->error and
         croakf("[%s] remote find command failed: %s",$host, $ssh->error);

    return $out;
}


