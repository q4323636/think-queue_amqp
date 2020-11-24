<?php
// +----------------------------------------------------------------------
// | ThinkPHP [ WE CAN DO IT JUST THINK IT ]
// +----------------------------------------------------------------------
// | Copyright (c) 2006-2015 http://thinkphp.cn All rights reserved.
// +----------------------------------------------------------------------
// | Licensed ( http://www.apache.org/licenses/LICENSE-2.0 )
// +----------------------------------------------------------------------
// | Author: yunwuxin <448901948@qq.com>
// +----------------------------------------------------------------------

namespace think\queue\command;

use think\Cache;
use think\console\Command;
use think\queue\InteractsWithTime;

class Restart extends Command
{
    use InteractsWithTime;

    protected function configure()
    {
        $this->setName('queue:restart')
            ->setDescription('Restart queue worker daemons after their current job');
    }

    public function handle(Cache $cache)
    {
        $cache->set('think:queue:restart', $this->currentTime());

        //用于解决队列重启放入的空队列
        $restartQueue = cache('think:queue:restartQueue');
        //var_dump($restartQueue);
        cache('think:queue:restartQueue', [], 86400);
        if (!empty($restartQueue)){
            $pushs = $this->app['queue']->connection("amqp");
            foreach ($restartQueue as $qname) {
                $pushs->push("app\common\job\DefaultQueue", '', $qname);
            }
        }
        
        $this->output->info("Broadcasting queue restart signal.");
    }
}
