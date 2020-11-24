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

namespace think\queue\connector;

use Closure;
use Exception;
use think\helper\Str;
use think\queue\Connector;
use think\queue\InteractsWithTime;
use think\queue\job\Amqp as AmqpJob;
use think\queue\Worker;

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

use think\facade\Log;

class Amqp extends Connector
{
    use InteractsWithTime;

    /** @var  \Amqp */
    protected $amqp;

    /**
     * The name of the default queue.
     *
     * @var string
     */
    protected $default;

    /**
     * The expiration time of a job.
     *
     * @var int|null
     */
    protected $retryAfter = 60;

    /**
     * The maximum number of seconds to block for a job.
     *
     * @var int|null
     */
    protected $blockFor = null;

    protected $msg = null;

    public function __construct(AMQPChannel $channel, $default = 'default', $retryAfter = 60, $blockFor = null)
    {
        $this->amqp      = $channel;
        $this->default    = $default;
        $this->retryAfter = $retryAfter;
        $this->blockFor   = $blockFor;
    }

    public static function __make($config)
    {
        if (!extension_loaded('sockets')) {
            throw new Exception('sockets扩展未安装');
        }

        if(stripos($config['host'], ',') !== false) {
            $links = [];
            $options = [];
            $hosts = explode(',',$config['host']);
            $rsArray = array_values(array_unique(array_diff($hosts, [""])));
            foreach ($hosts as $key=>$val){
                $links[] = ['host' => $val, 'port' => $config['port'], 'user' => $config['username'], 'password' => $config['password'], 'vhost' => $config['vhost']];
            }
            $amqp = AMQPStreamConnection::create_connection($links, $options);
        }else{
            $amqp = new AMQPStreamConnection($config['host'], $config['port'], $config['username'], $config['password'], $config['vhost']);
        }


        $channel = $amqp->channel();

        return new self($channel, $config['queue'], $config['retry_after'] ?? 60, $config['block_for'] ?? null);
    }

    public function size($queue)
    {
        //$queue = $this->getQueue($queue);

        return 0;
    }

    public function push($job, $data = '', $queue = null)
    {
        return $this->pushRaw($this->createPayload($job, $data), $queue);
    }

    public function pushRaw($payload, $queue = null, array $options = [])
    {
        $queue_name = $this->getQueue($queue);
        $topic_name = $queue_name.'_topic';

        $this->amqp->exchange_declare($topic_name, 'topic', false, true, false);
        $this->amqp->queue_declare($queue_name, false, true, false, false);
        $this->amqp->queue_bind($queue_name, $topic_name);

        $msg = new AMQPMessage($payload);
        $this->amqp->basic_publish($msg, $topic_name);

        //测试延迟队列
        //$this->release($queue, $payload, 0, 2);

        return json_decode($payload, true)['id'] ?? null;
    }

    public function later($delay, $job, $data = '', $queue = null)
    {
        $payload = $this->createPayload($job, $data);

        $queue_name = $this->getQueue($queue);

        if (empty($delay)) {
            $delay = 5 * 1000;
        }else{
            $delay = $delay * 1000;
        }

        $topic_name = $queue_name.'_topic';
        $queue_name_delay = $queue_name.$delay.'_delay';
        $topic_name_delay = $queue_name.$delay.'_topic_delay';


        $this->amqp->exchange_declare($topic_name, 'topic', false, true, false);
        $this->amqp->queue_declare($queue_name, false, true, false, false);
        $this->amqp->queue_bind($queue_name, $topic_name);

        $option = new AMQPTable();
        $option->set('x-message-ttl', $delay);
        $option->set('x-dead-letter-exchange', $topic_name);
        //$option->set('x-dead-letter-routing-key','routing_key');

        $this->amqp->exchange_declare($topic_name_delay, 'topic', false, true, false);
        $this->amqp->queue_declare($queue_name_delay, false, true, false, false, false, $option);
        $this->amqp->queue_bind($queue_name_delay, $topic_name_delay);

        $msg = new AMQPMessage($payload);
        $this->amqp->basic_publish($msg, $topic_name_delay);

        return json_decode($payload, true)['id'] ?? null;
    }

    public function pop($queue = null)
    {

        if (empty($nextJob = $this->popAmqp($queue))) {
            return;
        }
        list($nextJob,$msg) = $nextJob;
        if ($nextJob) {
            $job = $nextJob;
            return new AmqpJob($this->app, $this, $job, $this->connection, $queue,$msg);
        }

    }

    //拉取模式,默认模式
    public function popAmqp($queue = null)
    {   //Log::info("Queue:step:3");

//        $e = new Exception;
//        var_dump($e->getTraceAsString()); exit();

        if (!is_null($this->blockFor)) {
            return;
        }

        $prefixed = $queue_name = $this->getQueue($queue);
        $topic_name = $queue_name.'_topic';
        $this->amqp->exchange_declare($topic_name, 'topic', false, true, false);
        $this->amqp->queue_declare($queue_name, false, true, false, false);
        $this->amqp->queue_bind($queue_name, $topic_name);

        //拉取模式
        $this->amqp->basic_qos(0, 1, false);
        $msg = $this->amqp->basic_get($queue_name,false);
        if (!empty($msg)) {
            $job = $msg->body;
            //保证顺序执行，将确认放在最后处理
            //$this->amqp->basic_ack($msg->delivery_info['delivery_tag']);

            $this->blockFor = 1;
            return [$job,$msg];
        }

        return;

    }

    //消费者模式，amqp模式下
    //echo date('Y-m-d H:i:s')." [x] Received",$msg->body,PHP_EOL;
    public function consume(Worker $work, $lastRestart, $connection, $queue = null, $delay = 0, $sleep = 3, $maxTries = 0)
    {
        //记录队列名称，用于停止队列时插入空队列
        $restartQueue = cache('think:queue:restartQueue');
        if (empty($restartQueue)){
            $restartQueue = [];
        }
        $restartQueue[$queue] = $queue;
        cache('think:queue:restartQueue', $restartQueue, 86400);


        //echo date('Y-m-d H:i:s')." [x] Received-","consume",PHP_EOL;
        $queue_name = $this->getQueue($queue);
        $topic_name = $queue_name.'_topic';
        $this->amqp->exchange_declare($topic_name, 'topic', false, true, false);
        $this->amqp->queue_declare($queue_name, false, true, false, false);
        $this->amqp->queue_bind($queue_name, $topic_name);

        $_this = $this;
        //消费者模式
        $callback = function($msg) use ($_this, $work, $lastRestart, $connection, $queue, $delay, $maxTries){
            //echo date('Y-m-d H:i:s')." [x] Received",$msg->body,PHP_EOL;

            $nextJob = $msg->body;
            if ($nextJob) {
                $job = $nextJob;
                $amqpJob = new AmqpJob($_this->app, $_this, $job, $_this->connection, $queue,$msg);

                $result = $work->amqpAction($amqpJob, $lastRestart, $connection, $queue, $delay);
            }
        };

        //只有consumer已经处理并确认了上一条message时queue才分派新的message给它
        $this->amqp->basic_qos(0, 1, false);
        $this->amqp->basic_consume($queue_name,'',false,false,false,false, $callback);
        while (count($this->amqp->callbacks)) {
            $this->amqp->wait();
        }
    }


    /**
     * 重新发布任务--尽量少用，会将消息排在最后，
     *
     * @param string   $queue
     * @param StdClass $job
     * @param int      $delay
     * @return mixed
     */
    public function release($queue, $job, $delay)
    {   Log::info("Queue:step:4");
        //return $this->pushToAmqp($queue, $job->payload, $delay, $job->attempts);
        return;
    }

    /**
     * Push a raw payload to the Amqp with a given delay.
     *
     * @param \DateTime|int $delay
     * @param string|null   $queue
     * @param string        $payload
     * @param int           $attempts
     * @return mixed
     */
    protected function pushToAmqp($queue, $payload, $delay = 0, $attempts = 0)
    {
        $payload = $this->setMeta($payload, 'attempts', ++$attempts);

        $queue_name = $this->getQueue($queue);

        if (empty($delay)) {
            $delay = 5 * 1000;
        }else{
            $delay = $delay * 1000;
        }

        $queue_name_delay = $queue_name.$delay.'_delay';
        $topic_name_delay = $queue_name.$delay.'_topic_delay';

        $option = new AMQPTable();
        $option->set('x-message-ttl', $delay);
        $option->set('x-dead-letter-exchange', $queue_name.'_topic');
        //$option->set('x-dead-letter-routing-key','routing_key');

        $this->amqp->exchange_declare($topic_name_delay, 'topic', false, true, false);
        $this->amqp->queue_declare($queue_name_delay, false, true, false, false, false, $option);
        $this->amqp->queue_bind($queue_name_delay, $topic_name_delay);

        $msg = new AMQPMessage($payload);
        $this->amqp->basic_publish($msg, $topic_name_delay);

    }

    public function deleteReserved($queue, $job, $msg)
    {
        //确认消息
        $this->blockFor = null;
        $this->amqp->basic_ack($msg->delivery_info['delivery_tag']);
        return;
    }



    protected function createPayloadArray($job, $data = '')
    {
        return array_merge(parent::createPayloadArray($job, $data), [
            'id'       => $this->getRandomId(),
            'attempts' => 0,
        ]);
    }

    /**
     * 随机id
     *
     * @return string
     */
    protected function getRandomId()
    {
        return Str::random(32);
    }

    /**
     * 获取队列名
     *
     * @param string|null $queue
     * @return string
     */
    protected function getQueue($queue)
    {
        return 'queues_' . ($queue ?: $this->default);
    }
}
