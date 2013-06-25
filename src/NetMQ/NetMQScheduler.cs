using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NetMQ
{
	public class NetMQScheduler : TaskScheduler, IDisposable
	{
		private readonly BlockingCollection<Task> m_tasksToRun = new BlockingCollection<Task>();
		private readonly CancellationTokenSource m_cancellationTokenSource = new CancellationTokenSource();
		private readonly Task m_scheduledTask;
		private int m_threadId;

		public NetMQScheduler()
		{
			m_scheduledTask = Task.Factory.StartNew(ProcessTasks, TaskCreationOptions.LongRunning);
		}

		private void ProcessTasks()
		{
			m_threadId = Thread.CurrentThread.ManagedThreadId;
			try
			{
				foreach (var task in m_tasksToRun.GetConsumingEnumerable(m_cancellationTokenSource.Token))
					TryExecuteTask(task);
			}
			catch (OperationCanceledException)//Appears on Dispose
			{
			}
		}

		protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
		{
			return (m_threadId == Thread.CurrentThread.ManagedThreadId) && TryExecuteTask(task);
		}

		public override int MaximumConcurrencyLevel
		{
			get { return 1; }
		}

		public void Dispose()
		{
			m_tasksToRun.CompleteAdding();
			m_cancellationTokenSource.Cancel();
			m_scheduledTask.Wait();
			m_tasksToRun.Dispose();
		}

		protected override IEnumerable<Task> GetScheduledTasks()
		{
			return m_tasksToRun.ToArray();
		}

		protected override void QueueTask(Task task)
		{
			m_tasksToRun.Add(task);
		}
	}
}
