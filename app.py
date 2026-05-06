import streamlit as st
import pymysql
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
TABLE = "FnVCashCollectionChennai"

USERS = {
    "admin@ninjacart.com":                      {"password": "Admin@123",         "name": "Admin",            "facilities": "all"},
    "naveenarumugam@ninjacart.com":             {"password": "123456",            "name": "Naveen Arumugam",  "facilities": "all"},
    "varatharajan@ninjacart.com":               {"password": "Varatharajan@123",  "name": "Varadharajan",     "facilities": "all"},
    "kjayalakshmi947@gmail.com":               {"password": "Kjayalakshmi947@123",       "name": "Jayalakshmi",   "facilities": [9722]},
    "hmonavi564@gmail.com":                    {"password": "Hmonavi564@123",            "name": "Monavi",        "facilities": [2829]},
    "shivubujji849@gmail.com":                 {"password": "Shivubujji849@123",         "name": "Shivubujji",    "facilities": [9663]},
    "somsbond007@gmail.com":                   {"password": "Somsbond007@123",           "name": "Soms",          "facilities": [9662]},
    "ramesshy1503@gmail.com":                  {"password": "Ramesshy1503@123",          "name": "Ramesh",        "facilities": [9565]},
    "vishuvarthankuppusamy@ninjacart.com":      {"password": "Vishuvarthankuppusamy@123", "name": "Vishu Varthan", "facilities": [4572]},
    "adarshsony03141@gmail.com":               {"password": "Adarshsony03141@123",       "name": "Adarsh",        "facilities": [9592]},
    "krishnankrishna7480@gmail.com":           {"password": "Krishnankrishna7480@123",   "name": "Krishnan",      "facilities": [2773]},
    "yallusnayak5@gmail.com":                  {"password": "Yallusnayak5@123",          "name": "Yallus Nayak",  "facilities": [2038]},
    "rajeshraju6560@gmail.com":                {"password": "Rajeshraju6560@123",        "name": "Rajesh",        "facilities": [9555]},
    "nageshag45@gmail.com":                    {"password": "Nageshag45@123",            "name": "Nagesh",        "facilities": [4571]},
    "amaresha@ninjacart.com":                  {"password": "Amaresha@123",              "name": "Amaresha",      "facilities": [1851]},
    "chandrashakar702@gmail.com":              {"password": "Chandrashakar702@123",      "name": "Chandrashakar", "facilities": [5054]},
    "ma9986296393@gamil.com":                  {"password": "Ma9986296393@123",          "name": "MA",            "facilities": [759]},
    "boddureddy@ninjacart.com":                {"password": "Boddureddy@123",            "name": "Boddu Reddy",   "facilities": [9476]},
    "gurukirans7@gmail.com":                   {"password": "Gurukirans7@123",           "name": "Gurukiran",     "facilities": [1352]},
    "sunny.9738108777@gmail.com":              {"password": "Sunny.9738108777@123",      "name": "Sunny",         "facilities": [474]},
    "srinivasseenu1019@gmail.com":             {"password": "Srinivasseenu1019@123",     "name": "Srinivas",      "facilities": [2051]},
    "ganeshg5012@gmail.com":                   {"password": "Ganeshg5012@123",           "name": "Ganesh",        "facilities": [3727]},
    "reddyashokkumar964@gamil.com":            {"password": "Reddyashokkumar964@123",    "name": "Ashok Kumar",   "facilities": [4222]},
    "dineshdkdineshdk21@gmail.com":            {"password": "Dineshdkdineshdk21@123",    "name": "Dinesh",        "facilities": [923]},
    "kiranchinnu0230@gmail.com":               {"password": "Kiranchinnu0230@123",       "name": "Kiran",         "facilities": [4224]},
    "koushiknagaraj28@gmail.com":              {"password": "Koushiknagaraj28@123",      "name": "Koushik Nagaraj","facilities": [9645]},
    "chowdaiahp50@gmail.com":                  {"password": "Chowdaiahp50@123",          "name": "Chowdaiah",     "facilities": [4265]},
    "ammun670@gmail.com":                      {"password": "Ammun670@123",              "name": "Ammun",         "facilities": [4225]},
}

PAYMENT_STATUS_OPTIONS  = ["Paid", "Not Paid"]
COLLECTION_WINDOW_OPT   = ["Time of Delivery", "Before 5", "After 5", "After 7", "After 9", "Next Day"]


# ─────────────────────────────────────────────
# DATABASE HELPERS
# ─────────────────────────────────────────────
def get_connection():
    cfg = {
        "host":     os.getenv("DB_HOST"),
        "port":     int(os.getenv("DB_PORT", 3306)),
        "user":     os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": os.getenv("DB_NAME"),
        "charset":  "utf8mb4",
    }
    if not cfg["host"]:
        cfg = {
            "host":     st.secrets["DB_HOST"],
            "port":     int(st.secrets["DB_PORT"]),
            "user":     st.secrets["DB_USER"],
            "password": st.secrets["DB_PASSWORD"],
            "database": st.secrets["DB_NAME"],
            "charset":  "utf8mb4",
        }
    return pymysql.connect(**cfg)


def run_query(sql, params=None):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
        return pd.DataFrame(rows, columns=cols)
    finally:
        conn.close()


def run_write(sql, params=None):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
    finally:
        conn.close()


# ─────────────────────────────────────────────
# DATA FETCH
# ─────────────────────────────────────────────
@st.cache_data(ttl=60)
def get_delivery_dates():
    df = run_query(f"SELECT DISTINCT DeliveryDate FROM {TABLE} ORDER BY DeliveryDate DESC")
    return df["DeliveryDate"].tolist()


@st.cache_data(ttl=60)
def get_facilities(delivery_date, allowed_facilities=None):
    if allowed_facilities and allowed_facilities != "all":
        placeholders = ",".join(["%s"] * len(allowed_facilities))
        df = run_query(
            f"SELECT DISTINCT FacilityId, Facility FROM {TABLE} "
            f"WHERE DeliveryDate = %s AND FacilityId IN ({placeholders}) ORDER BY Facility",
            params=(delivery_date, *allowed_facilities),
        )
    else:
        df = run_query(
            f"SELECT DISTINCT FacilityId, Facility FROM {TABLE} "
            f"WHERE DeliveryDate = %s ORDER BY Facility",
            params=(delivery_date,),
        )
    return df


@st.cache_data(ttl=60)
def get_drivers(delivery_date, facility_id, mode):
    df = run_query(
        f"SELECT DISTINCT DriverId, Driver FROM {TABLE} "
        f"WHERE DeliveryDate = %s AND FacilityId = %s AND OrderMode = %s "
        f"AND Driver IS NOT NULL ORDER BY Driver",
        params=(delivery_date, facility_id, mode),
    )
    return df


def get_customers(delivery_date, facility_id, mode, driver_id=None):
    query = (
        f"SELECT SaleOrderId, CustomerId, Customer, InvoiceAmount, UPIAmount, CashAmount, "
        f"PaymentStatus, CollectionWindow "
        f"FROM {TABLE} "
        f"WHERE DeliveryDate = %s AND FacilityId = %s AND OrderMode = %s"
    )
    params = [delivery_date, facility_id, mode]
    if driver_id:
        query += " AND DriverId = %s"
        params.append(driver_id)
    query += " ORDER BY Customer"
    return run_query(query, params=params)


# ─────────────────────────────────────────────
# LOGIN
# ─────────────────────────────────────────────
def show_login():
    st.set_page_config(page_title="CHN Cash Collection", page_icon="💰", layout="centered")
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("## 💰 CHN Cash Collection")
        st.markdown("##### Ops Executive Login")
        st.divider()
        email    = st.text_input("Mail ID", placeholder="Enter your email")
        password = st.text_input("Password", type="password", placeholder="Enter your password")
        if st.button("Login", use_container_width=True, type="primary"):
            email_lower = email.strip().lower()
            matched_key = next((k for k in USERS if k.lower() == email_lower), None)
            if matched_key and USERS[matched_key]["password"] == password.strip():
                st.session_state["logged_in"]    = True
                st.session_state["username"]     = matched_key
                st.session_state["display_name"] = USERS[matched_key]["name"]
                st.session_state["facilities"]   = USERS[matched_key]["facilities"]
                st.rerun()
            else:
                st.error("Invalid email or password.")


# ─────────────────────────────────────────────
# MAIN APP
# ─────────────────────────────────────────────
def show_app():
    st.set_page_config(page_title="CHN Cash Collection", page_icon="💰", layout="wide")
    with st.sidebar:
        st.markdown(f"### 👤 {st.session_state['display_name']}")
        st.divider()
        page = st.radio("Navigation", ["📝 Update Collection", "📊 View Records"])
        st.divider()
        if st.button("🚪 Logout"):
            st.session_state.clear()
            st.rerun()

    if page == "📝 Update Collection":
        show_update_collection()
    else:
        show_view_records()


# ─────────────────────────────────────────────
# UPDATE COLLECTION PAGE
# ─────────────────────────────────────────────
def show_update_collection():
    st.title("📝 Update Cash Collection")
    st.divider()

    # ── Filters ──
    col1, col2 = st.columns(2)
    with col1:
        delivery_dates = get_delivery_dates()
        if not delivery_dates:
            st.warning("No data available.")
            return
        delivery_date = st.selectbox(
            "📅 Delivery Date",
            delivery_dates,
            format_func=lambda d: d.strftime("%d %b %Y") if hasattr(d, "strftime") else str(d),
        )
    with col2:
        facilities_df = get_facilities(delivery_date, st.session_state.get("facilities"))
        if facilities_df.empty:
            st.warning("No facilities found.")
            return
        facility_map = {row["FacilityId"]: f"{row['FacilityId']} — {row['Facility']}"
                        for _, row in facilities_df.iterrows()}
        facility_id = st.selectbox(
            "🏭 Facility",
            options=list(facility_map.keys()),
            format_func=lambda fid: facility_map[fid],
        )

    col1, col2 = st.columns(2)
    with col1:
        mode = st.selectbox("🚚 Mode", ["Delivery", "Pickup"])
    with col2:
        if mode == "Delivery":
            drivers_df = get_drivers(delivery_date, facility_id, mode)
            if drivers_df.empty:
                st.info("No drivers found.")
                driver_id = None
            else:
                driver_map = {None: "All Drivers"}
                driver_map.update({row["DriverId"]: row["Driver"].strip()
                                   for _, row in drivers_df.iterrows()})
                driver_id = st.selectbox(
                    "🧑‍✈️ Driver",
                    options=list(driver_map.keys()),
                    format_func=lambda did: driver_map[did],
                )
        else:
            driver_id = None
            st.text_input("🧑‍✈️ Driver", value="N/A (Pickup)", disabled=True)

    # ── Summary ──
    customers_df = get_customers(delivery_date, facility_id, mode, driver_id)
    if customers_df.empty:
        st.warning("No customers found.")
        return

    total       = len(customers_df)
    filled      = customers_df["PaymentStatus"].notna().sum()
    not_filled  = total - filled

    st.divider()
    st.markdown(f"""
    <div style="display:flex;gap:12px;flex-wrap:nowrap;overflow-x:auto;padding:8px 0">
        <div style="flex:1;min-width:100px;background:#f0f2f6;border-radius:8px;padding:12px;text-align:center">
            <div style="font-size:11px;color:#666">Total Customers</div>
            <div style="font-size:22px;font-weight:700">{total}</div>
        </div>
        <div style="flex:1;min-width:100px;background:#d4edda;border-radius:8px;padding:12px;text-align:center">
            <div style="font-size:11px;color:#666">Updated</div>
            <div style="font-size:22px;font-weight:700;color:#155724">{int(filled)}</div>
        </div>
        <div style="flex:1;min-width:100px;background:#f8d7da;border-radius:8px;padding:12px;text-align:center">
            <div style="font-size:11px;color:#666">Pending Update</div>
            <div style="font-size:22px;font-weight:700;color:#721c24">{int(not_filled)}</div>
        </div>
        <div style="flex:1;min-width:100px;background:#f0f2f6;border-radius:8px;padding:12px;text-align:center">
            <div style="font-size:11px;color:#666">Total Invoice (₹)</div>
            <div style="font-size:18px;font-weight:700">₹{customers_df['InvoiceAmount'].sum():,.0f}</div>
        </div>
        <div style="flex:1;min-width:100px;background:#f0f2f6;border-radius:8px;padding:12px;text-align:center">
            <div style="font-size:11px;color:#666">Total Cash (₹)</div>
            <div style="font-size:18px;font-weight:700">₹{customers_df['CashAmount'].sum():,.0f}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Customer Table with inline update ──
    st.divider()
    st.markdown("#### 👤 Customer List")
    st.caption("All customers are editable — click any row to update or change a previously saved status.")

    col_f1, col_f2 = st.columns(2)
    with col_f1:
        show_pending_only = st.checkbox("Show only pending (not updated)", value=False)
    with col_f2:
        show_paid_only = st.checkbox("Show only Paid", value=False)

    display_df = customers_df.copy()
    if show_pending_only:
        display_df = display_df[display_df["PaymentStatus"].isna()]
    elif show_paid_only:
        display_df = display_df[display_df["PaymentStatus"] == "Paid"]

    if display_df.empty:
        st.success("✅ All customers have been updated!")
        return

    for _, row in display_df.iterrows():
        order_id   = int(row["SaleOrderId"])
        cust_name  = row["Customer"].strip()
        invoice    = row["InvoiceAmount"] or 0
        cash       = row["CashAmount"] or 0
        upi        = row["UPIAmount"] or 0
        cur_status = row["PaymentStatus"]
        cur_window = row["CollectionWindow"]

        # Icon + label based on status
        if pd.isna(cur_status):
            icon = "⏳"
            status_label = "Not Updated"
        elif cur_status == "Paid":
            icon = "✅"
            status_label = "Paid"
        else:
            icon = "❌"
            status_label = "Not Paid"

        with st.expander(
            f"{icon} {cust_name} | Invoice: ₹{invoice:,.0f} | Cash: ₹{cash:,.0f}"
            f" | UPI: ₹{upi:,.0f} | {status_label}",
            expanded=(cur_status != "Paid"),
        ):
            if cash <= 0:
                st.info(f"💳 UPI/Wallet payment only (₹{upi:,.0f}) — no cash collection required.")
                continue

            if cur_status == "Paid":
                st.success(f"✅ Marked as **Paid** | Collection Window: {cur_window or '—'}")
                continue

            # Not yet Paid — allow update
            status_idx = PAYMENT_STATUS_OPTIONS.index(cur_status) if cur_status in PAYMENT_STATUS_OPTIONS else 0
            new_status = st.selectbox(
                "Payment Status",
                PAYMENT_STATUS_OPTIONS,
                index=status_idx,
                key=f"status_{order_id}",
            )

            # Show Time of Delivery only when record was never touched (NULL) and user selects Paid
            # If previously set to Not Paid, or selecting Not Paid → exclude Time of Delivery
            if new_status == "Paid" and pd.isna(cur_status):
                window_opts = COLLECTION_WINDOW_OPT
            else:
                window_opts = [w for w in COLLECTION_WINDOW_OPT if w != "Time of Delivery"]

            window_idx = window_opts.index(cur_window) if cur_window in window_opts else 0
            new_window = st.selectbox(
                "Collection Window",
                window_opts,
                index=window_idx,
                key=f"window_{order_id}",
            )

            if st.button("💾 Save", key=f"save_{order_id}", type="primary"):
                try:
                    run_write(
                        f"UPDATE {TABLE} SET PaymentStatus=%s, CollectionWindow=%s, "
                        f"UpdatedBy=%s, UpdatedAt=%s WHERE SaleOrderId=%s",
                        params=(
                            new_status, new_window,
                            st.session_state["username"], datetime.now(),
                            order_id,
                        ),
                    )
                    st.success(f"✅ Saved — {cust_name}: {new_status} | {new_window}")
                    get_delivery_dates.clear()
                    get_facilities.clear()
                    get_drivers.clear()
                    st.rerun()
                except Exception as ex:
                    st.error(f"Database error: {ex}")


# ─────────────────────────────────────────────
# VIEW RECORDS PAGE
# ─────────────────────────────────────────────
def show_view_records():
    st.title("📊 Collection Records")
    st.divider()

    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        delivery_dates = get_delivery_dates()
        filter_date = st.selectbox(
            "Filter by Delivery Date",
            [None] + delivery_dates,
            format_func=lambda d: "All Dates" if d is None else (
                d.strftime("%d %b %Y") if hasattr(d, "strftime") else str(d)
            ),
        )
    with col2:
        if filter_date:
            facilities_df = get_facilities(filter_date, st.session_state.get("facilities"))
            fac_map = {None: "All Facilities"}
            fac_map.update({row["FacilityId"]: row["Facility"]
                            for _, row in facilities_df.iterrows()})
            filter_facility = st.selectbox(
                "Filter by Facility",
                options=list(fac_map.keys()),
                format_func=lambda k: fac_map[k],
            )
        else:
            filter_facility = None
            st.selectbox("Filter by Facility", ["All Facilities"], disabled=True)
    with col3:
        st.markdown("")
        st.markdown("")
        if st.button("🔄 Refresh"):
            get_delivery_dates.clear()
            get_facilities.clear()
            get_drivers.clear()
            st.rerun()

    where  = ["1=1"]
    params = []
    if filter_date:
        where.append("DeliveryDate = %s")
        params.append(filter_date)
    if filter_facility:
        where.append("FacilityId = %s")
        params.append(filter_facility)

    # Restrict to user's facilities
    allowed = st.session_state.get("facilities")
    if allowed and allowed != "all":
        placeholders = ",".join(["%s"] * len(allowed))
        where.append(f"FacilityId IN ({placeholders})")
        params.extend(allowed)

    df = run_query(
        f"SELECT * FROM {TABLE} WHERE {' AND '.join(where)} ORDER BY DeliveryDate DESC, Facility, Customer",
        params=params if params else None,
    )

    if df.empty:
        st.info("No records found.")
        return

    # Summary metrics
    total    = len(df)
    paid     = df[df["PaymentStatus"] == "Paid"].shape[0]
    not_paid = df[df["PaymentStatus"] == "Not Paid"].shape[0]
    pending  = df["PaymentStatus"].isna().sum()

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Total Orders",    total)
    m2.metric("Paid",            paid)
    m3.metric("Not Paid",        not_paid)
    m4.metric("Pending Update",  int(pending))
    m5.metric("Total Cash (₹)",  f"₹{df['CashAmount'].sum():,.2f}")

    st.divider()

    tab1, tab2 = st.tabs(["📋 Customer-wise Detail", "🧑‍✈️ Driver-wise Summary"])

    with tab1:
        st.dataframe(
            df.rename(columns={
                "DeliveryDate": "Date", "FacilityId": "Fac. ID",
                "CustomerId": "Cust. ID", "SaleOrderId": "Order ID",
                "OrderMode": "Mode", "InvoiceAmount": "Invoice (₹)",
                "UPIAmount": "UPI (₹)", "CashAmount": "Cash (₹)",
                "PaymentStatus": "Status", "CollectionWindow": "Collection Window",
                "UpdatedBy": "Updated By", "UpdatedAt": "Updated At",
            }),
            use_container_width=True, hide_index=True,
        )
        csv1 = df.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Download CSV", data=csv1,
                           file_name="chn_collection_detail.csv", mime="text/csv")

    with tab2:
        driver_summary = run_query(f"""
            SELECT
                Driver, Facility,
                COUNT(DISTINCT SaleOrderId)                      AS TotalOrders,
                SUM(InvoiceAmount)                               AS TotalInvoice,
                SUM(CashAmount)                                  AS TotalCash,
                SUM(CASE WHEN PaymentStatus='Paid' THEN 1 ELSE 0 END)     AS Paid,
                SUM(CASE WHEN PaymentStatus='Not Paid' THEN 1 ELSE 0 END) AS NotPaid,
                SUM(CASE WHEN PaymentStatus IS NULL THEN 1 ELSE 0 END)    AS PendingUpdate
            FROM {TABLE}
            WHERE {' AND '.join(where)}
            GROUP BY Driver, Facility
            ORDER BY Facility, Driver
        """, params=params if params else None)

        st.dataframe(
            driver_summary.rename(columns={
                "Driver": "Driver", "Facility": "Facility",
                "TotalOrders": "Total Orders",
                "TotalInvoice": "Invoice (₹)", "TotalCash": "Cash (₹)",
                "Paid": "Paid", "NotPaid": "Not Paid", "PendingUpdate": "Pending Update",
            }),
            use_container_width=True, hide_index=True,
        )
        csv2 = driver_summary.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Download Driver Summary", data=csv2,
                           file_name="chn_driver_summary.csv", mime="text/csv")


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__" or True:
    if not st.session_state.get("logged_in"):
        show_login()
    else:
        show_app()
