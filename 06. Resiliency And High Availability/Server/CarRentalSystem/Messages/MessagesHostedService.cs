namespace CarRentalSystem.Messages
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Data.Models;
    using Hangfire;
    using MassTransit;
    using Microsoft.EntityFrameworkCore;
    using Microsoft.Extensions.Hosting;
    using Microsoft.Extensions.DependencyInjection;

    public class MessagesHostedService : IHostedService
    {
        private readonly IRecurringJobManager recurringJob;
        //private readonly DbContext data;
        private readonly IServiceProvider serviceProvider;
        private readonly IBus publisher;

        public MessagesHostedService(
            IRecurringJobManager recurringJob,
            IServiceProvider serviceProvider, 
            IBus publisher)
        {
            this.recurringJob = recurringJob;
            //this.data = data;
            this.serviceProvider = serviceProvider;
            this.publisher = publisher;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            this.recurringJob.AddOrUpdate(
                nameof(MessagesHostedService),
                () => this.ProcessPendingMessages(),
                "*/5 * * * * *");

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
            => Task.CompletedTask;

        public void ProcessPendingMessages()
        {
            using var data = this.serviceProvider
                .CreateScope()
                .ServiceProvider
                .GetService<DbContext>();

            var messages = data
                .Set<Message>()
                .Where(m => !m.Published)
                .OrderBy(m => m.Id)
                .ToList();

            foreach (var message in messages)
            {
                this.publisher
                    .Publish(message.Data, message.Type)
                    .GetAwaiter()
                    .GetResult();

                message.MarkAsPublished();

                //this.data.SaveChanges();
                data.SaveChanges();
            }
        }
    }
}
