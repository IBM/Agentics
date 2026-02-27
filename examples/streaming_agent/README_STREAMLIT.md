# Streamlit Chat Client for AGStream

A modern web-based chat interface for interacting with AGStream agents via Kafka.

## Features

- 💬 **Real-time Chat Interface**: Clean, modern chat UI built with Streamlit
- 🔄 **Conversation History**: Maintains context across messages
- ⚙️ **Configurable Timeout**: Adjust response timeout via sidebar
- 📊 **Live Stats**: Track message count and session info
- 🗑️ **Clear History**: Reset conversation with one click
- 🎨 **Responsive Design**: Works on desktop and mobile

## Prerequisites

1. **Kafka Server**: Running on `localhost:9092` (or configured via `.env`)
2. **AGStream Listener**: Must be running to process messages
3. **Streamlit**: Install with `pip install streamlit`

## Installation

```bash
# Install Streamlit if not already installed
pip install streamlit

# Or install all dependencies
pip install -r requirements.txt
```

## Usage

### 1. Start the AGStream Listener

First, start the listener that will process your messages:

```bash
python examples/streaming_agent/start_listener.py
```

### 2. Launch the Streamlit Chat Client

In a new terminal, run:

```bash
streamlit run examples/streaming_agent/streamlit_chat_client.py
```

This will:
- Open your default browser automatically
- Display the chat interface at `http://localhost:8501`
- Connect to Kafka topics for messaging

### 3. Start Chatting!

- Type your message in the input box at the bottom
- Press Enter or click Send
- Wait for the agent's response (default timeout: 30 seconds)
- Continue the conversation with full context

## Configuration

### Environment Variables

Configure via `.env` file:

```bash
KAFKA_SERVER=localhost:9092
KAFKA_INPUT_TOPIC=agentics-chat-input
KAFKA_OUTPUT_TOPIC=agentics-chat-output
```

### Sidebar Settings

- **Response Timeout**: Adjust how long to wait for agent replies (5-60 seconds)
- **Clear Chat History**: Reset the conversation
- **Stats**: View message count and session ID

## Architecture

```
┌─────────────────┐         ┌──────────────┐         ┌─────────────────┐
│   Streamlit     │         │    Kafka     │         │   AGStream      │
│   Chat Client   │────────▶│   Topics     │────────▶│   Listener      │
│                 │         │              │         │                 │
│  (User Input)   │         │  input-topic │         │  (Processing)   │
│                 │◀────────│ output-topic │◀────────│                 │
└─────────────────┘         └──────────────┘         └─────────────────┘
```

## Features Explained

### Conversation History
The client maintains full conversation context, allowing the agent to reference previous messages.

### Timeout Handling
If the agent doesn't respond within the timeout period, you'll see a warning message. You can:
- Increase the timeout in the sidebar
- Check if the listener is running
- Verify Kafka connectivity

### Session Management
Each browser session gets a unique ID, allowing multiple users to chat simultaneously without interference.

## Troubleshooting

### "No reply received within X seconds"

**Possible causes:**
- Listener is not running → Start `start_listener.py`
- Listener is processing slowly → Increase timeout
- Kafka connection issues → Check Kafka server

### "Error: Connection refused"

**Solution:**
- Ensure Kafka is running: `docker ps` or check your Kafka service
- Verify `KAFKA_SERVER` in `.env` matches your setup

### Messages not appearing

**Solution:**
- Check that topics exist: Run the client once to auto-create them
- Verify listener is consuming from correct topics
- Check Kafka logs for errors

## Comparison with CLI Client

| Feature | CLI Client | Streamlit Client |
|---------|-----------|------------------|
| Interface | Terminal | Web Browser |
| History Display | Text only | Rich UI with formatting |
| Configuration | Code/env only | Interactive sidebar |
| Multi-user | No | Yes (separate sessions) |
| Mobile-friendly | No | Yes |
| Stats/Monitoring | No | Yes |

## Advanced Usage

### Custom Styling

Modify the Streamlit theme by creating `.streamlit/config.toml`:

```toml
[theme]
primaryColor = "#FF4B4B"
backgroundColor = "#FFFFFF"
secondaryBackgroundColor = "#F0F2F6"
textColor = "#262730"
font = "sans serif"
```

### Running on Different Port

```bash
streamlit run examples/streaming_agent/streamlit_chat_client.py --server.port 8502
```

### Running on Network

```bash
streamlit run examples/streaming_agent/streamlit_chat_client.py --server.address 0.0.0.0
```

## Next Steps

- Customize the UI styling
- Add message export functionality
- Implement user authentication
- Add support for file uploads
- Create multi-agent chat rooms

## Support

For issues or questions:
1. Check the main AGStream documentation
2. Verify Kafka and listener are running
3. Check browser console for errors
4. Review Streamlit logs in terminal

---

**Made with ❤️ using AGStream and Streamlit**
