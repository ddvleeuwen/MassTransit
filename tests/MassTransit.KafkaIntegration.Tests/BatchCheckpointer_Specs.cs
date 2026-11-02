namespace MassTransit.KafkaCheckpointTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Threading;
    using System.Threading.Tasks;
    using Confluent.Kafka;
    using KafkaIntegration;
    using KafkaIntegration.Checkpoints;
    using NUnit.Framework;


    [TestFixture]
    public class BatchCheckpointer_Specs
    {
        [Test]
        public async Task Should_not_commit_past_a_canceled_message()
        {
            var consumer = CommitRecorder.Create(out var commits);
            var partition = new TopicPartition("topic", 0);

            var checkpointer = new BatchCheckpointer(consumer, new Settings(), CancellationToken.None);

            var confirmations = Enumerable.Range(0, 4).Select(i => new PendingConfirmation(partition, i)).ToArray();
            foreach (var confirmation in confirmations)
                await checkpointer.Pending(confirmation);

            // offsets 0 and 2 completed, 1 and 3 canceled (still queued when the consumer stopped)
            confirmations[0].Complete();
            confirmations[2].Complete();
            confirmations[1].Canceled(new CancellationToken(true));
            confirmations[3].Canceled(new CancellationToken(true));

            await checkpointer.DisposeAsync();

            Assert.That(commits.Select(x => x.Offset.Value), Is.EqualTo(new long[] { 1 }));
        }

        [Test]
        public async Task Should_commit_after_last_completed_message()
        {
            var consumer = CommitRecorder.Create(out var commits);
            var partition = new TopicPartition("topic", 0);

            var checkpointer = new BatchCheckpointer(consumer, new Settings(), CancellationToken.None);

            var confirmations = Enumerable.Range(0, 3).Select(i => new PendingConfirmation(partition, i)).ToArray();
            foreach (var confirmation in confirmations)
                await checkpointer.Pending(confirmation);

            confirmations[2].Complete();
            confirmations[0].Complete();
            confirmations[1].Complete();

            await checkpointer.DisposeAsync();

            Assert.That(commits.Last().Offset.Value, Is.EqualTo(3));
        }


        class Settings :
            ReceiveSettings
        {
            public long Offset => 0;
            public string Topic => "topic";
            public ushort MessageLimit => 100;
            public ushort CheckpointMessageCount => 100;
            public int PrefetchCount => 100;
            public int ConcurrentDeliveryLimit => 1;
            public int ConcurrentMessageLimit => 4;
            public ushort ConcurrentConsumerLimit => 1;
            public TimeSpan CheckpointInterval => TimeSpan.FromSeconds(10);
        }


        public class CommitRecorder :
            DispatchProxy
        {
            readonly List<TopicPartitionOffset> _commits = new List<TopicPartitionOffset>();

            public static IConsumer<byte[], byte[]> Create(out List<TopicPartitionOffset> commits)
            {
                var proxy = Create<IConsumer<byte[], byte[]>, CommitRecorder>();
                commits = ((CommitRecorder)(object)proxy)._commits;
                return proxy;
            }

            protected override object Invoke(MethodInfo targetMethod, object[] args)
            {
                if (targetMethod.Name == nameof(IConsumer<byte[], byte[]>.Commit) && args.Length == 1
                    && args[0] is IEnumerable<TopicPartitionOffset> offsets)
                {
                    lock (_commits)
                        _commits.AddRange(offsets);
                    return null;
                }

                if (targetMethod.Name == "get_" + nameof(IConsumer<byte[], byte[]>.MemberId))
                    return "member";

                return targetMethod.ReturnType.IsValueType ? Activator.CreateInstance(targetMethod.ReturnType) : null;
            }
        }
    }
}
