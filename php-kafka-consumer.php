<?php
namespace app\commands;

use app\modules\common\services\KafkaService;
use yii\console\Controller;

class KafkaConsumerController extends Controller
{

	/**
	 * kafka消费者(手动提交)
	 */
	public function actionConsumer($topic)
	{
		pcntl_signal(SIGHUP, [$this,"sigHandler"]);
		pcntl_signal(SIGINT, [$this,"sigHandler"]);
		pcntl_signal(SIGQUIT, [$this,"sigHandler"]);
		pcntl_signal(SIGTERM, [$this,"sigHandler"]);
		$conf = new \RdKafka\Conf();
		
		// Set a rebalance callback to log partition assignments (optional)
		$conf->setRebalanceCb(function (\RdKafka\KafkaConsumer $kafka, $err, array $partitions = null) {
			switch ($err) {
				case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
					echo "Assign: ";
					// var_dump($partitions);
					$kafka->assign($partitions);
					break;
				
				case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
					echo "Revoke: ";
					// var_dump($partitions);
					$kafka->assign(NULL);
					break;
				
				default:
					throw new \Exception($err);
			}
		});
		
		// Configure the group.id. All consumer with the same group.id will consume
		// different partitions.
		
		// Initial list of Kafka brokers
		$conf->set('metadata.broker.list', \Yii::$app->params['kafka_borker']);
		$conf->set('group.id', $topic);
		$conf->set('log.connection.close', 'false');
		$conf->set('enable.auto.commit', 'false');
		$conf->set('api.version.request', 'true');
		$topicConf = new \RdKafka\TopicConf();
		
		// Set where to start consuming messages when there is no initial offset in
		// offset store or the desired offset is out of range.
		// 'smallest': start from the beginning
		$topicConf->set('auto.offset.reset', 'earliest');
		// $topicConf->set('auto.commit.enable', 'false');
		// $topicConf->set('auto.commit.interval.ms', 100);
		// Set the configuration to use for subscribed/assigned topics
		$conf->setDefaultTopicConf($topicConf);
		
		$consumer = new \RdKafka\KafkaConsumer($conf);
		
		// Subscribe to topic 'test'
		$consumer->subscribe([$topic]);
		
		echo "Waiting for partition assignment... (make take some time when\n";
		echo "quickly re-joining the group after leaving it.)\n";
		while (true)
		{
			$message = $consumer->consume(120 * 1000);
			switch ($message->err) {
				case RD_KAFKA_RESP_ERR_NO_ERROR:
					$className = '\app\modules\tools\kafka\\' . ucfirst($topic);
					$class = new $className();
					call_user_func_array(array($class,'run'), ['data' => json_decode($message->payload, true)]);
					try
					{//如果失败尝试提交两次（防节点崩溃）
						$consumer->commit();
					}
					catch (\Exception $e)
					{
						try
						{
							$consumer->commit();
						}
						catch (\Exception $e)
						{
							$consumer->commit();
						}
					}
					pcntl_signal_dispatch();
					break;
				case RD_KAFKA_RESP_ERR__PARTITION_EOF:
					echo "will wait for more..\n";
					pcntl_signal_dispatch();
					break;
				case RD_KAFKA_RESP_ERR__TIMED_OUT:
					echo "Timed out\n";
					pcntl_signal_dispatch();
					break;
				default:
					throw new \Exception($message->errstr(), $message->err);
					break;
			}
		}
	}

	private function sigHandler($signo)
	{
		switch ($signo) {
			case SIGHUP:
			case SIGQUIT:
			case SIGTERM:
			case SIGINT:
				echo 'shutdown';
				exit();
				break;
			default:
		}
	}
	/**
	 * kafka消费者(自动提交)
	 */
	public function actionAutoConsumer($topic)
	{
		pcntl_signal(SIGHUP, [$this,"sigHandler"]);
		pcntl_signal(SIGINT, [$this,"sigHandler"]);
		pcntl_signal(SIGQUIT, [$this,"sigHandler"]);
		pcntl_signal(SIGTERM, [$this,"sigHandler"]);
		$conf = new \RdKafka\Conf();
	
		// Set a rebalance callback to log partition assignments (optional)
		$conf->setRebalanceCb(function (\RdKafka\KafkaConsumer $kafka, $err, array $partitions = null) {
			switch ($err) {
				case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
					echo "Assign: ";
					// var_dump($partitions);
					$kafka->assign($partitions);
					break;
	
				case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
					echo "Revoke: ";
					// var_dump($partitions);
					$kafka->assign(NULL);
					break;
	
				default:
					throw new \Exception($err);
			}
		});
	
			// Configure the group.id. All consumer with the same group.id will consume
			// different partitions.
	
			// Initial list of Kafka brokers
			$conf->set('metadata.broker.list', \Yii::$app->params['kafka_borker']);
			$conf->set('group.id', $topic);
			$conf->set('log.connection.close', 'false');
			$conf->set('api.version.request', 'true');
			$topicConf = new \RdKafka\TopicConf();
	
			// Set where to start consuming messages when there is no initial offset in
			// offset store or the desired offset is out of range.
			// 'smallest': start from the beginning
			$topicConf->set('auto.offset.reset', 'earliest');
			// $topicConf->set('auto.commit.enable', 'false');
			// $topicConf->set('auto.commit.interval.ms', 100);
			// Set the configuration to use for subscribed/assigned topics
			$conf->setDefaultTopicConf($topicConf);
	
			$consumer = new \RdKafka\KafkaConsumer($conf);
	
			// Subscribe to topic 'test'
			$consumer->subscribe([$topic]);
	
			echo "Waiting for partition assignment... (make take some time when\n";
			echo "quickly re-joining the group after leaving it.)\n";
			while (true)
			{
				$message = $consumer->consume(120 * 1000);
				switch ($message->err) {
					case RD_KAFKA_RESP_ERR_NO_ERROR:
						$className = '\app\modules\tools\kafka\\' . ucfirst($topic);
						$class = new $className();
						call_user_func_array(array($class,'run'), ['data' => json_decode($message->payload, true)]);
						pcntl_signal_dispatch();
						break;
					case RD_KAFKA_RESP_ERR__PARTITION_EOF:
						echo "will wait for more..\n";
						pcntl_signal_dispatch();
						break;
					case RD_KAFKA_RESP_ERR__TIMED_OUT:
						echo "Timed out\n";
						pcntl_signal_dispatch();
						break;
					default:
						throw new \Exception($message->errstr(), $message->err);
						break;
				}
			}
	}

}
