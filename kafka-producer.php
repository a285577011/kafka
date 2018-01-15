<?php
namespace app\modules\common\services;

use app\modules\common\helpers\Commonfun;

class KafkaService
{

	private static $queData = [];

	public static $rdTopicInstance = null;

	/**
	 * 同步发送队列
	 * @param unknown $queName
	 * @param unknown $params
	 */
	public static function addQue($queName, array $params, $log = false)
	{
		if ($log)
		{
			$queId = Commonfun::addQueue('Kafka-' . $queName, $queName, $params);
			$params['queueId'] = $queId;
		}
		self::addKfQueue($queName, $params);
		return ['code' => 600,'msg' => '发送成功'];
	}

	/**
	 * rdkafka
	 * @param unknown $queName
	 * @param unknown $params
	 */
	public static function addKfQueue($queName, $params)
	{
			static $topic = null;
			if ($topic[$queName] === null)
			{
				$conf = new \RdKafka\Conf();
				$redis = \yii::$app->redis;
				$conf->setDrMsgCb(function ($kafka, $message) use($redis) {
					if ($message->err)
					{
						$redis->executeCommand('lpush', ['kafka-fail-que',var_export($message, true)]);
					}else
					{
						//success
					}
				});
				$rk = new \RdKafka\Producer($conf);
				$rk->setLogLevel(LOG_DEBUG);
				$rk->addBrokers(\Yii::$app->params['kafka_borker']);
				$topicConf = new \RdKafka\TopicConf();
				// 开启提交失败重复提交
				$topicConf->set("request.required.acks", 1); // 1 节点下线时候保证不会重复发送(可能丢失) -1要副本都确认 节点异常有可能节点会重复发(不会丢失)
				$topic[$queName] = $rk->newTopic($queName, $topicConf);
			}
			$res=$topic[$queName]->produce(RD_KAFKA_PARTITION_UA, 0, json_encode($params));
			//$rk->poll(0);
		return $res;
	}
}
