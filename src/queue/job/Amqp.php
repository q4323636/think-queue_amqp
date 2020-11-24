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

namespace think\queue\job;

use think\App;
use think\queue\connector\Amqp as AmqpQueue;
use think\queue\Job;

class Amqp extends Job
{

    /**
     * The amqp queue instance.
     * @var AmqpQueue
     */
    protected $amqp;

    /**
     * The database job payload.
     * @var Object
     */
    protected $job;

    /**
     * The JSON decoded version of "$job".
     *
     * @var array
     */
    protected $decoded;

    protected $msg;

    public function __construct(App $app, AmqpQueue $amqp, $job, $connection, $queue,$msg)
    {
        $this->app        = $app;
        $this->job        = $job;
        $this->queue      = $queue;
        $this->connection = $connection;
        $this->amqp       = $amqp;
        $this->msg        = $msg;
        
        $this->decoded = $this->payload();
    }

    /**
     * Get the number of times the job has been attempted.
     * @return int
     */
    public function attempts()
    {
        return ($this->decoded['attempts'] ?? null) + 1;
    }

    /**
     * Get the raw body string for the job.
     * @return string
     */
    public function getRawBody()
    {
        return $this->job;
    }

    /**
     * 删除任务
     *
     * @return void
     */
    public function delete()
    {
        parent::delete();

        $this->amqp->deleteReserved($this->queue, $this, $this->msg);
    }

    /**
     * 重新发布任务
     *
     * @param int $delay
     * @return void
     */
    public function release($delay = 0)
    {
        parent::release($delay);

        $this->amqp->release($this->queue, $this, $delay);
    }

    /**
     * Get the job identifier.
     *
     * @return string
     */
    public function getJobId()
    {
        return $this->decoded['id'] ?? null;
    }

}
