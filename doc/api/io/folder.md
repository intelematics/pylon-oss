# Pylon Inputs and Outputs

## Folder
Pylon can get input messages from files under a specific path. The folder should have no other files in it (excluding files beginning with ".") and files are deleted once the corresponding message has been processed. Pylon can also output one message per file to a specific folder. Combining these two makes for a very easy way to chain pylon components together locally.

It is important to keep in mind Pylon will delete files as it processes the messages in them, so keep a copy if you want to reuse the messages.

### Example
Create these four files:
- component1.py
    ```python
    import pylon


    @pylon.SourceComponent
    def main(message, config):
        for phrase in config['PHRASES']:
            out = pylon.models.messages.RawContentMessage('text/plain')
            out.body = phrase
            yield out


    if __name__ == '__main__':
        main.runOnce()
    ```
- component2.py
    ```python
    import pylon


    @pylon.SinkComponent
    def main(message, config):
        print(f'\nMessage received: {message.body}\n')


    if __name__ == '__main__':
        main.runForever()
    ```
- config1.json
    ```json
    {
        "PHRASES": [
            "Beautiful is better than ugly.",
            "Explicit is better than implicit.",
            "Simple is better than complex.",
            "Complex is better than complicated.",
            "Flat is better than nested.",
            "Sparse is better than dense.",
            "Readability counts.",
            "Special cases aren't special enough to break the rules.",
            "Although practicality beats purity.",
            "Errors should never pass silently.",
            "Unless explicitly silenced.",
            "In the face of ambiguity, refuse the temptation to guess.",
            "There should be one-- and preferably only one --obvious way to do it.",
            "Although that way may not be obvious at first unless you're Dutch.",
            "Now is better than never.",
            "Although never is often better than *right* now.",
            "If the implementation is hard to explain, it's a bad idea.",
            "If the implementation is easy to explain, it may be a good idea.",
            "Namespaces are one honking great idea -- let's do more of those!"
        ],
        "PYLON_OUTPUT": "folder://component2"
    }
    ```
- config2.json
    ```json
    {
        "PYLON_INPUT": "folder://component2/",
        "PYLON_LOOP_SLEEP_SECONDS": 5
    }
    ```

Then run these commands in seperate terminal windows.
- `TASK_CONFIG=$(cat config1.json) python component1.py`
- `TASK_CONFIG=$(cat config2.json) python component2.py`

In this example we are creating two components: a source component which feeds into a sink component. When the source component runs you should notice the folder `component2` fills up with files, this is the source component output. When the sink component is run it will slowly process each of those messages, deleting the corresponding files as it goes and printing the message body.

This feature is paticularly useful for local development. If for whatever reason you would like to
deploy a component and then feed it the messages you had for testing locally, here is a short script
to help (untested, but hopefully enough to get you started, please update if it doesn't work).

```python
import pylon

DIR = 'path/to/files'
# Replace with any message consumer, sqs is just an example
MESSAGE_CONSUMER = pylon.aws.sqs.Queue('queue_name')

filepaths = pylon.io.folder._listFilepaths(DIR)
for filepath in filepaths:
    with open(filepath) as _fd
        raw_message = _fd.read()
    message = pylon.io.folder._decode(raw_message)
    MESSAGE_CONSUMER.sendMessage(message)
```
