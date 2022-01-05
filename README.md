## Count kafka messages count
If you want to count the number of message sin kafka topic without using KSQL you may want to define a windowed state store with kafka streams.

Your stream will aggregate the messages over windowed durations.

Finally, all you would need to do is fetch windows (between two times) and grab the count of messages in each window.