<?php
namespace app\modules\common\services;

use app\modules\common\helpers\Commonfun;

class KafkaService
{

	private static $queData = [];

	public static $rdTopicInstance = null;

	const LOG_PREFIX = 'Log';

	const LOG_NAME = 'BusinessLog';

	/**
	 * 异步发送队列
	 * @param unknown $queName
	 * @param unknown $params
	 */
	public static function addQue($queName, array $params, $log = true)
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
	private static function addKfQueue($queName, $params)
	{
		//static $topic = null;
		$queName=self::getSubTopic($queName);
		$conf = new \RdKafka\Conf();
		$conf->set('log.connection.close', 'false'); // 防止断开连接
		$conf->set('api.version.request', 'true'); // api请求版本
		$redis = \yii::$app->redis;
		$conf->setDrMsgCb(function ($kafka, $message) use ($redis, $queName, $params) {
			if ($message->err)
			{
				$redis->executeCommand('lpush', ['kafka-fail-queData',json_encode(['topic' => $queName,'data' => $params])]);
				$redis->executeCommand('lpush', ['kafka-fail-que',var_export($message, true)]);
			}else
			{
				// success
			}
		});
		$rk = new \RdKafka\Producer($conf);
		// $rk->setLogLevel(LOG_DEBUG);
		$rk->addBrokers(\Yii::$app->params['kafka_borker']);
		$topicConf = new \RdKafka\TopicConf();
		// 开启提交失败重复提交
		$topicConf->set("request.required.acks", 1); // 1 节点下线时候保证不会重复发送(可能丢失) -1要副本都确认 节点异常有可能节点会重复发(不会丢失)
		                                             // $topicConf->set("message.timeout.ms", 1000); // 消息超时时间
		//if (!isset($topic[$queName]))
		//{
			$topic = $rk->newTopic($queName, $topicConf);
		//}
		$res = $topic->produce(RD_KAFKA_PARTITION_UA, 0, json_encode($params));
		$rk->poll(0);
		return $res;
	}

	/**
	 * rdkafa(添加日志)
	 * @param string $logName 日志名称(主题名)
	 * @param unknown $data(数据)
	 */
	public static function addLog($logName, $data, $now = false)
	{
		if ($now)
		{
			$tableName = 'business_log';
			if (YII_ENV_DEV)
			{
				$tableName .= '_dev';
			}
			\Yii::$app->db2->createCommand()->insert($tableName, ['type' => $logName,'data' => var_export($data, true),'log_ctime' => time(),'c_time' => time()])->execute();
			return;
		}
		$topicName = self::LOG_PREFIX . '-' . self::LOG_NAME;
		//static $topic = null;
		$conf = new \RdKafka\Conf();
		$conf->set('log.connection.close', 'false'); // 防止断开连接
		$conf->set('api.version.request', 'true'); // api请求版本
		$redis = \yii::$app->redis;
		$conf->setDrMsgCb(function ($kafka, $message) use ($redis) {
			if ($message->err)
			{
				$redis->executeCommand('lpush', ['kafka-fail-que',var_export($message, true)]);
			}else
			{
				// success
			}
		});
		$rk = new \RdKafka\Producer($conf);
		// $rk->setLogLevel(LOG_DEBUG);
		$rk->addBrokers(\Yii::$app->params['kafka_borker']);
		$topicConf = new \RdKafka\TopicConf();
		// 开启提交失败重复提交
		$topicConf->set("request.required.acks", 0); // 日志可靠性级别可以降低，提高速度
		//if (!isset($topic[$topicName]))
		//{
			$topic = $rk->newTopic($topicName, $topicConf);
		//}
		$lodata = ['data' => $data,'topic' => $logName,'c_time' => time()];
		$res = $topic->produce(RD_KAFKA_PARTITION_UA, 0, json_encode($lodata));
		$rk->poll(0);
		return $res;
	}
	/**
	 * 获取订阅的主题名称
	 */
	public static function getSubTopic($topic){
		$subTopic='Gula-'.$topic;
		return $subTopic;
	}
}
