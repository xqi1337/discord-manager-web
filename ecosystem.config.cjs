const path = require("path");

const cwd = __dirname;

module.exports = {
  apps: [
    {
      name: "discord-manager-web",
      cwd,
      script: path.join(cwd, ".venv/bin/python"),
      args: "app.py",
      interpreter: "none",
      instances: 1,
      exec_mode: "fork",
      watch: false,
      time: true,
      max_restarts: 5,
      restart_delay: 5000,
      merge_logs: true,
      out_file: path.join(cwd, "logs/out.log"),
      error_file: path.join(cwd, "logs/error.log"),
      env: {
        HOST: "127.0.0.1",
        PORT: "5000",
        FLASK_DEBUG: "0",
        PYTHONUNBUFFERED: "1",
        DISCORD_API_BASE_URL: "https://discord.com/api/v9",
        DISCORD_REQUEST_TIMEOUT: "10",
        DISCORD_MAX_RETRIES: "3",
        DISCORD_REQUEST_SPACING: "0.25",
        DISCORD_SEARCH_MAX_PAGES: "0",
        DISCORD_SEARCH_INDEX_RETRIES: "3",
        CHANNEL_MESSAGE_PAGE_SIZE: "20",
        TASK_MESSAGE_LOG_LIMIT: "1000",
        DM_EXPORT_MAX_PAGES: "0",
        DM_EXPORT_ASSET_TIMEOUT: "20",
        DM_EXPORT_MAX_ASSET_BYTES: "26214400",
      },
    },
  ],
};
