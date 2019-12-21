<?php

require __DIR__.'/../vendor/autoload.php';

use Illuminate\Contracts\Console\Kernel;
use Illuminate\Foundation\Application;
use Illuminate\Queue\Jobs\SqsJob;
use Illuminate\Queue\QueueManager;
use Illuminate\Queue\SqsQueue;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\Log;

lambda(function ($event) {
    try {
        /** @var Application $application */
        $application = require __DIR__.'/../bootstrap/app.php';

        /** @var Kernel $kernel */
        $kernel = $application->make(Kernel::class);
        $kernel->bootstrap();

        try {
            /** @var SqsQueue $sqsQueue */
            $sqsQueue = $application
                ->make(QueueManager::class)
                ->connection('sqs');
        } catch (Throwable $throwable) {
            Log::error(
                'Could not resolve SQSQueue from QueueManager',
                ['exception' => $throwable]
            );

            return 1;
        }

        foreach (Arr::get($event, 'Records') as $record) {
            try {
                $record = collect($record)
                    ->mapWithKeys(
                        function ($value, $key) {
                            return [ucfirst($key) => $value];
                        }
                    );

                // Given: xxx:yyy:zzz:queue-name => queue-name
                $queue = Arr::last(
                    explode(':', Arr::get($record, 'EventSourceARN'))
                );

                $sqsJob = new SqsJob(
                    $application,
                    $sqsQueue->getSqs(),
                    $record->toArray(),
                    null,
                    $sqsQueue->getQueue($queue)
                );

                $sqsJob->fire();
            } catch (Exception $exception) {
                Log::error(
                    "Job: {$exception->getMessage()}",
                    ['exception' => $exception]
                );

                return 1;
            }
        }
    } catch (Throwable $throwable) {
        Log::error(
            "Lambda: {$throwable->getMessage()}",
            ['exception' => $throwable]
        );

        return 1;
    }

    return 0;
});
