(function () {
    function storageAvailable() {
        try {
            window.localStorage.setItem("discord-manager-storage-test", "1");
            window.localStorage.removeItem("discord-manager-storage-test");
            return true;
        } catch (error) {
            return false;
        }
    }

    function confirmForms() {
        document.querySelectorAll("form[data-confirm]").forEach(function (form) {
            form.addEventListener("submit", function (event) {
                var message = form.getAttribute("data-confirm") || "Confirmer ?";
                if (!window.confirm(message)) {
                    event.preventDefault();
                }
            });
        });
    }

    function selectAllButtons() {
        document.querySelectorAll("[data-select-all]").forEach(function (button) {
            button.addEventListener("click", function () {
                var name = button.getAttribute("data-select-all");
                var boxes = Array.from(document.querySelectorAll('input[name="' + name + '"]'));
                if (!boxes.length) {
                    return;
                }

                var shouldCheck = boxes.some(function (box) {
                    return !box.checked;
                });
                boxes.forEach(function (box) {
                    box.checked = shouldCheck;
                });
            });
        });
    }

    function themeMode() {
        var button = document.querySelector("[data-theme-toggle]");
        if (!button) {
            return;
        }

        var label = button.querySelector("[data-theme-label]");
        var icon = button.querySelector("[data-theme-icon]");
        var canStore = storageAvailable();

        function currentTheme() {
            return document.documentElement.getAttribute("data-theme") === "dark" ? "dark" : "light";
        }

        function apply(theme) {
            var nextTheme = theme === "dark" ? "dark" : "light";
            var isDark = nextTheme === "dark";
            document.documentElement.setAttribute("data-theme", nextTheme);
            button.setAttribute("aria-pressed", isDark ? "true" : "false");
            button.classList.toggle("active", isDark);
            if (label) {
                label.textContent = isDark ? "Theme clair" : "Theme sombre";
            }
            if (icon) {
                icon.className = isDark ? "bi bi-sun" : "bi bi-moon-stars";
            }
            if (canStore) {
                window.localStorage.setItem("discord-manager-theme", nextTheme);
            }
        }

        button.addEventListener("click", function () {
            apply(currentTheme() === "dark" ? "light" : "dark");
        });

        apply(currentTheme());
    }

    function opsecMode() {
        var button = document.querySelector("[data-opsec-toggle]");
        if (!button) {
            return;
        }

        var label = button.querySelector("[data-opsec-label]");
        var canStore = storageAvailable();

        function sync() {
            var enabled = document.documentElement.classList.contains("opsec-enabled");
            button.setAttribute("aria-pressed", enabled ? "true" : "false");
            button.classList.toggle("active", enabled);
            if (label) {
                label.textContent = enabled ? "OPSEC actif" : "OPSEC";
            }
            if (canStore) {
                window.localStorage.setItem("discord-manager-opsec", enabled ? "1" : "0");
            }
        }

        button.addEventListener("click", function () {
            document.documentElement.classList.toggle("opsec-enabled");
            sync();
        });

        sync();
    }

    function cssEscape(value) {
        if (window.CSS && typeof window.CSS.escape === "function") {
            return window.CSS.escape(value);
        }
        return String(value).replace(/["\\]/g, "\\$&");
    }

    function selectedChannelId() {
        var params = new URLSearchParams(window.location.search);
        if (params.get("selected")) {
            return params.get("selected");
        }

        if (window.location.hash && window.location.hash.startsWith("#channel-")) {
            return window.location.hash.replace("#channel-", "");
        }

        return "";
    }

    function selectChannelFromUrl() {
        var channelId = selectedChannelId();
        if (!channelId) {
            return;
        }

        var checkbox = document.querySelector('input[name="channels"][value="' + cssEscape(channelId) + '"]');
        if (checkbox) {
            checkbox.checked = true;
            checkbox.scrollIntoView({ block: "center" });
        }
    }

    function setText(root, field, value) {
        var element = root.querySelector('[data-field="' + field + '"]');
        if (element) {
            element.textContent = value === null || value === undefined || value === "" ? "En attente" : String(value);
        }
    }

    function updateTaskStatus(root, payload) {
        var statusBadge = root.querySelector('[data-field="status_label"]');
        if (statusBadge) {
            statusBadge.textContent = payload.status_label;
            statusBadge.className = "badge " + payload.status_badge_class;
        }

        var progress = Math.max(0, Math.min(100, Number(payload.progress || 0)));
        var progressBar = root.querySelector('[data-field="progress_bar"]');
        if (progressBar) {
            var label = progress.toFixed(1) + "%";
            progressBar.style.width = label;
            progressBar.textContent = label;
        }

        setText(root, "total_messages", payload.total_messages);
        setText(root, "deleted_messages", payload.deleted_messages);
        setText(root, "failed_messages", payload.failed_messages);
        setText(root, "skipped_messages", payload.skipped_messages);
        setText(root, "current_channel_name", payload.current_channel_name);
        setText(root, "current_message", payload.current_message);
        setText(root, "start_time", payload.start_time);
        setText(root, "end_time", payload.end_time);

        var errorBox = root.querySelector('[data-field="error_box"]');
        var errorText = root.querySelector('[data-field="error"]');
        if (errorBox && errorText) {
            if (payload.error) {
                errorText.textContent = payload.error;
                errorBox.classList.remove("d-none");
            } else {
                errorBox.classList.add("d-none");
            }
        }

        updateExportStatus(payload);
        updateMessageLogs(payload);
    }

    function createLogEntry(entry) {
        var item = document.createElement("article");
        item.className = "task-message-item";

        var header = document.createElement("header");

        var channel = document.createElement("strong");
        channel.setAttribute("data-opsec", "");
        channel.textContent = entry.channel_name || entry.channel_id || "Salon inconnu";
        header.appendChild(channel);

        var time = document.createElement("span");
        time.textContent = entry.timestamp || "";
        header.appendChild(time);

        item.appendChild(header);

        var content = document.createElement("p");
        content.setAttribute("data-opsec", "");
        content.textContent = entry.content || "(sans texte)";
        item.appendChild(content);

        var meta = document.createElement("div");
        meta.className = "task-message-meta";

        var type = document.createElement("span");
        type.className = "badge text-bg-light";
        type.textContent = entry.type_label || entry.type || "Message";
        meta.appendChild(type);

        if (entry.attachments) {
            var attachments = document.createElement("span");
            attachments.className = "badge text-bg-light";
            attachments.textContent = entry.attachments + " piece(s) jointe(s)";
            meta.appendChild(attachments);
        }

        if (entry.embeds) {
            var embeds = document.createElement("span");
            embeds.className = "badge text-bg-light";
            embeds.textContent = entry.embeds + " embed(s)";
            meta.appendChild(embeds);
        }

        if (entry.jump_url) {
            var link = document.createElement("a");
            link.className = "btn btn-sm btn-outline-secondary";
            link.href = entry.jump_url;
            link.target = "_blank";
            link.rel = "noreferrer noopener";

            var icon = document.createElement("i");
            icon.className = "bi bi-box-arrow-up-right";
            link.appendChild(icon);

            var text = document.createElement("span");
            text.textContent = "Lien";
            link.appendChild(text);

            meta.appendChild(link);
        }

        item.appendChild(meta);
        return item;
    }

    function renderLogList(root, entries, emptyText) {
        root.innerHTML = "";
        if (!entries || !entries.length) {
            var empty = document.createElement("p");
            empty.className = "section-subtitle mb-0";
            empty.textContent = emptyText;
            root.appendChild(empty);
            return;
        }

        entries.forEach(function (entry) {
            root.appendChild(createLogEntry(entry));
        });
    }

    function updateMessageLogs(payload) {
        var panel = document.getElementById("task-message-log");
        if (!panel) {
            return;
        }

        var found = panel.querySelector('[data-field="found_message_log"]');
        var deleted = panel.querySelector('[data-field="deleted_message_log"]');
        var failed = panel.querySelector('[data-field="failed_message_log"]');
        var limited = panel.querySelector('[data-field="message_log_limited"]');

        if (found && Array.isArray(payload.found_message_log)) {
            renderLogList(found, payload.found_message_log, "Aucun message trouve pour le moment.");
        }

        if (deleted && Array.isArray(payload.deleted_message_log)) {
            renderLogList(deleted, payload.deleted_message_log, "Aucun message supprime pour le moment.");
        }

        if (failed && Array.isArray(payload.failed_message_log)) {
            renderLogList(failed, payload.failed_message_log, "Aucun echec pour le moment.");
        }

        if (limited) {
            limited.classList.toggle("d-none", !payload.message_log_limited);
        }
    }

    function updateExportStatus(payload) {
        var exportPanel = document.getElementById("task-exports");
        if (!exportPanel || !payload.export_before_delete) {
            return;
        }

        var statusBadge = exportPanel.querySelector('[data-field="export_status_label"]');
        if (statusBadge) {
            statusBadge.textContent = payload.export_status_label || "Non demande";
            statusBadge.className = "badge " + (payload.export_status_badge_class || "text-bg-secondary");
        }

        var filesRoot = exportPanel.querySelector('[data-field="export_files"]');
        if (filesRoot && Array.isArray(payload.export_files)) {
            filesRoot.innerHTML = "";
            if (!payload.export_files.length) {
                var empty = document.createElement("p");
                empty.className = "section-subtitle mb-0";
                empty.textContent = "Aucun ZIP disponible pour le moment.";
                filesRoot.appendChild(empty);
            } else {
                payload.export_files.forEach(function (exportFile) {
                    var item = document.createElement("a");
                    item.className = "export-link";
                    item.href = exportFile.download_url;
                    item.setAttribute("download", exportFile.filename);

                    var icon = document.createElement("i");
                    icon.className = "bi bi-file-zip";
                    item.appendChild(icon);

                    var text = document.createElement("span");
                    text.setAttribute("data-opsec", "");
                    text.textContent = (exportFile.name || exportFile.filename) + " - " + (exportFile.message_count || 0) + " messages";
                    item.appendChild(text);

                    filesRoot.appendChild(item);
                });
            }
        }

        var errorsRoot = exportPanel.querySelector('[data-field="export_errors"]');
        if (errorsRoot && Array.isArray(payload.export_errors)) {
            errorsRoot.classList.toggle("d-none", !payload.export_errors.length);
            errorsRoot.textContent = payload.export_errors.map(function (error) {
                return error.name + ": " + error.error;
            }).join(" | ");
        }
    }

    function channelResultSearch() {
        var input = document.querySelector("[data-channel-search]");
        if (!input) {
            return;
        }

        var results = Array.from(document.querySelectorAll("[data-channel-result]"));
        input.addEventListener("input", function () {
            var query = input.value.trim().toLowerCase();
            results.forEach(function (result) {
                var text = result.getAttribute("data-channel-search-text") || "";
                result.classList.toggle("d-none", Boolean(query) && !text.includes(query));
            });
        });
    }

    function updateMessagePager(pager, nextPage) {
        var pageCount = Number(pager.getAttribute("data-page-count") || "1");
        var page = Math.max(1, Math.min(pageCount, nextPage));
        pager.setAttribute("data-current-page", String(page));

        var scope = pager.closest("[data-message-scope]") || pager.closest(".collapse");
        if (scope) {
            scope.querySelectorAll("[data-channel-message]").forEach(function (message) {
                message.classList.toggle("is-hidden", Number(message.getAttribute("data-message-page")) !== page);
            });
        }

        var label = pager.querySelector("[data-page-label]");
        if (label) {
            label.textContent = "Page " + page + " / " + pageCount;
        }

        var prev = pager.querySelector("[data-page-prev]");
        var next = pager.querySelector("[data-page-next]");
        if (prev) {
            prev.disabled = page <= 1;
        }
        if (next) {
            next.disabled = page >= pageCount;
        }
    }

    function messagePagers() {
        document.querySelectorAll("[data-message-pager]").forEach(function (pager) {
            var current = Number(pager.getAttribute("data-current-page") || "1");
            updateMessagePager(pager, current);

            var prev = pager.querySelector("[data-page-prev]");
            var next = pager.querySelector("[data-page-next]");
            if (prev) {
                prev.addEventListener("click", function () {
                    updateMessagePager(pager, Number(pager.getAttribute("data-current-page") || "1") - 1);
                });
            }
            if (next) {
                next.addEventListener("click", function () {
                    updateMessagePager(pager, Number(pager.getAttribute("data-current-page") || "1") + 1);
                });
            }
        });
    }

    function pollTaskStatus() {
        var root = document.getElementById("task-status");
        if (!root) {
            return;
        }

        var url = root.getAttribute("data-status-url");
        if (!url) {
            return;
        }

        var timer = window.setInterval(function () {
            window.fetch(url, { headers: { "Accept": "application/json" } })
                .then(function (response) {
                    if (!response.ok) {
                        throw new Error("HTTP " + response.status);
                    }
                    return response.json();
                })
                .then(function (payload) {
                    updateTaskStatus(root, payload);
                    if (payload.status === "completed" || payload.status === "error" || payload.status === "interrupted") {
                        window.clearInterval(timer);
                    }
                })
                .catch(function () {
                    window.clearInterval(timer);
                });
        }, 1500);
    }

    document.addEventListener("DOMContentLoaded", function () {
        confirmForms();
        selectAllButtons();
        themeMode();
        opsecMode();
        selectChannelFromUrl();
        channelResultSearch();
        messagePagers();
        pollTaskStatus();
    });
})();
