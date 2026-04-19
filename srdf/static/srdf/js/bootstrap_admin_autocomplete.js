(function () {

    function getBootstrapPlanAdminBaseUrl() {
        const path = window.location.pathname || "";

        const planMatch = path.match(/^(.*\/srdf\/bootstrapplan\/)(add\/|[0-9]+\/change\/)?$/);
        if (planMatch && planMatch[1]) {
            return planMatch[1];
        }

        const itemMatch = path.match(/^(.*\/srdf\/bootstrapplanitem\/)(add\/|[0-9]+\/change\/)?$/);
        if (itemMatch && itemMatch[1]) {
            return itemMatch[1].replace(/bootstrapplanitem\/$/, "bootstrapplan/");
        }

        return "/admin-api/srdf/bootstrapplan/";
    }

    function debounce(fn, delay) {
        let timer = null;
        return function () {
            const args = arguments;
            const ctx = this;
            clearTimeout(timer);
            timer = setTimeout(function () {
                fn.apply(ctx, args);
            }, delay);
        };
    }

    function ensureDatalist(input, listId) {
        let dl = document.getElementById(listId);
        if (!dl) {
            dl = document.createElement("datalist");
            dl.id = listId;
            document.body.appendChild(dl);
        }
        input.setAttribute("list", listId);
        return dl;
    }

    function renderOptions(datalist, items) {
        datalist.innerHTML = "";
        (items || []).forEach(function (item) {
            const opt = document.createElement("option");
            opt.value = item.value || "";
            opt.label = item.label || item.value || "";
            datalist.appendChild(opt);
        });
    }

    function getMainReplicationServiceId() {
        const el = document.getElementById("id_replication_service");
        return el ? el.value : "";
    }

    function getBootstrapPlanIdFromStandaloneItemForm() {
        const el = document.getElementById("id_bootstrap_plan");
        return el ? el.value : "";
    }

    async function fetchJson(url) {
        const resp = await fetch(url, { credentials: "same-origin" });
        const text = await resp.text();

        try {
            return JSON.parse(text);
        } catch (e) {
            console.error("[SRDF] Invalid JSON for URL:", url);
            console.error("[SRDF] Raw response:", text);
            throw e;
        }
    }

    async function resolveReplicationServiceId() {
        const mainId = getMainReplicationServiceId();
        if (mainId) {
            return mainId;
        }

        const bootstrapPlanId = getBootstrapPlanIdFromStandaloneItemForm();
        if (!bootstrapPlanId) {
            return "";
        }

        const url = getBootstrapPlanAdminBaseUrl() + "plan-service/?bootstrap_plan_id=" + encodeURIComponent(bootstrapPlanId);
        const data = await fetchJson(url);
        return data.replication_service_id || "";
    }

    function findScopedInputs() {
        return Array.from(
            document.querySelectorAll(
                "input[name$='source_database_name'], input[name$='table_name'], #id_source_database_name, #id_table_name"
            )
        );
    }

    function bindDatabaseInput(input) {
        const listId = input.id + "_db_datalist";
        const datalist = ensureDatalist(input, listId);

        const run = debounce(async function () {
            try {
                const replicationServiceId = await resolveReplicationServiceId();
                if (!replicationServiceId) {
                    renderOptions(datalist, []);
                    return;
                }

                const q = input.value || "";
                const url =
                    getBootstrapPlanAdminBaseUrl() +
                    "db-autocomplete/?replication_service_id=" +
                    encodeURIComponent(replicationServiceId) +
                    "&q=" +
                    encodeURIComponent(q);

                const data = await fetchJson(url);
                renderOptions(datalist, data.results || []);
            } catch (e) {
                console.error("[SRDF] db autocomplete error", e);
                renderOptions(datalist, []);
            }
        }, 250);

        input.addEventListener("focus", run);
        input.addEventListener("input", run);
    }

    function bindTableInput(input) {
        const listId = input.id + "_table_datalist";
        const datalist = ensureDatalist(input, listId);

        const run = debounce(async function () {
            try {
                const replicationServiceId = await resolveReplicationServiceId();
                if (!replicationServiceId) {
                    renderOptions(datalist, []);
                    return;
                }

                const row = input.closest("tr") || input.closest(".form-row") || document;
                let databaseInput = row.querySelector("input[name$='source_database_name']");
                if (!databaseInput) {
                    databaseInput = document.getElementById("id_source_database_name");
                }

                const databaseName = databaseInput ? databaseInput.value : "";
                if (!databaseName) {
                    renderOptions(datalist, []);
                    return;
                }

                const q = input.value || "";
                const url =
                    getBootstrapPlanAdminBaseUrl() +
                    "table-autocomplete/?replication_service_id=" +
                    encodeURIComponent(replicationServiceId) +
                    "&database_name=" +
                    encodeURIComponent(databaseName) +
                    "&q=" +
                    encodeURIComponent(q);

                const data = await fetchJson(url);
                renderOptions(datalist, data.results || []);
            } catch (e) {
                console.error("[SRDF] table autocomplete error", e);
                renderOptions(datalist, []);
            }
        }, 250);

        input.addEventListener("focus", run);
        input.addEventListener("input", run);
    }

    function boot() {
        const inputs = findScopedInputs();

        inputs.forEach(function (input) {
            const name = input.name || input.id || "";

            if (name.endsWith("source_database_name") || name === "id_source_database_name") {
                bindDatabaseInput(input);
            }

            if (name.endsWith("table_name") || name === "id_table_name") {
                bindTableInput(input);
            }
        });
    }

    document.addEventListener("DOMContentLoaded", boot);
})();