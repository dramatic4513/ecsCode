import psycopg2
import os
import re
from collections import Counter

host = "172.23.52.199"
port = 20004
database = "tpcdstest"
user = "postgres"
password = "postgres"
def attribute_to_table(attribute_name):
    connection = psycopg2.connect(host=host, port=port, database=database, user=user)
    cur = connection.cursor()
    cur.execute(
        """ SELECT relname FROM pg_class WHERE oid IN (  SELECT attrelid FROM pg_attribute  WHERE attname = %s );""",
        (attribute_name,))
    table_name = cur.fetchone()[0]
    # print(table_name)
    cur.close()
    connection.close()
    return table_name
def coder():
    table_name_list = ["call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer_address",
                  "customer", "customer_demographics", "date_dim", "dbgen_version", "household_demographics",
                  "income_band", "inventory", "item", "promotion", "reason", "ship_mode",
                  "store", "store_returns", "store_sales", "time_dim", "warehouse", "web_page", "web_returns", "web_sales",
                  "web_site"]
    # table_dict = dict()
    # for index, value in enumerate(table_name_list):
    #     table_dict[value] = index
    # print(table_dict)
    table_dict = {'call_center': 0, 'catalog_page': 1, 'catalog_returns': 2, 'catalog_sales': 3, 'customer_address': 4,
                  'customer': 5, 'customer_demographics': 6, 'date_dim': 7, 'dbgen_version': 8,
                  'household_demographics': 9, 'income_band': 10, 'inventory': 11, 'item': 12, 'promotion': 13,
                  'reason': 14, 'ship_mode': 15, 'store': 16, 'store_returns': 17, 'store_sales': 18, 'time_dim': 19,
                  'warehouse': 20, 'web_page': 21, 'web_returns': 22, 'web_sales': 23, 'web_site': 24}
    # connection = psycopg2.connect(host=host, port=port, database=database, user=user)
    # tables_columns = {}
    #
    # for table in table_name_list:
    #     cur = connection.cursor()
    #     cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = '{}'".format(table))
    #     columns = cur.fetchall()
    #
    #     # 为每个列编号，并存储在表对应的字典中
    #     columns_dict = {}
    #     for i, column in enumerate(columns):
    #         column_name = column[0]
    #         columns_dict[column_name] = i + 1
    #
    #     # 将表名和对应的列名编号字典存储在主字典中
    #     tables_columns[table] = columns_dict
    #     cur.close()
    # connection.close()
    # print(tables_columns)

    tables_columns = {'call_center': {'cc_call_center_sk': 1, 'cc_call_center_id': 2, 'cc_rec_start_date': 3, 'cc_rec_end_date': 4, 'cc_closed_date_sk': 5, 'cc_open_date_sk': 6, 'cc_name': 7, 'cc_class': 8, 'cc_employees': 9, 'cc_sq_ft': 10, 'cc_hours': 11, 'cc_manager': 12, 'cc_mkt_id': 13, 'cc_mkt_class': 14, 'cc_mkt_desc': 15, 'cc_market_manager': 16, 'cc_division': 17, 'cc_division_name': 18, 'cc_company': 19, 'cc_company_name': 20, 'cc_street_number': 21, 'cc_street_name': 22, 'cc_street_type': 23, 'cc_suite_number': 24, 'cc_city': 25, 'cc_county': 26, 'cc_state': 27, 'cc_zip': 28, 'cc_country': 29, 'cc_gmt_offset': 30, 'cc_tax_percentage': 31},
                      'catalog_page': {'cp_catalog_page_sk': 1, 'cp_catalog_page_id': 2, 'cp_start_date_sk': 3, 'cp_end_date_sk': 4, 'cp_department': 5, 'cp_catalog_number': 6, 'cp_catalog_page_number': 7, 'cp_description': 8, 'cp_type': 9},
                      'catalog_returns': {'cr_returned_date_sk': 1, 'cr_returned_time_sk': 2, 'cr_item_sk': 3, 'cr_refunded_customer_sk': 4, 'cr_refunded_cdemo_sk': 5, 'cr_refunded_hdemo_sk': 6, 'cr_refunded_addr_sk': 7, 'cr_returning_customer_sk': 8, 'cr_returning_cdemo_sk': 9, 'cr_returning_hdemo_sk': 10, 'cr_returning_addr_sk': 11, 'cr_call_center_sk': 12, 'cr_catalog_page_sk': 13, 'cr_ship_mode_sk': 14, 'cr_warehouse_sk': 15, 'cr_reason_sk': 16, 'cr_order_number': 17, 'cr_return_quantity': 18, 'cr_return_amount': 19, 'cr_return_tax': 20, 'cr_return_amt_inc_tax': 21, 'cr_fee': 22, 'cr_return_ship_cost': 23, 'cr_refunded_cash': 24, 'cr_reversed_charge': 25, 'cr_store_credit': 26, 'cr_net_loss': 27},
                      'catalog_sales': {'cs_sold_date_sk': 1, 'cs_sold_time_sk': 2, 'cs_ship_date_sk': 3, 'cs_bill_customer_sk': 4, 'cs_bill_cdemo_sk': 5, 'cs_bill_hdemo_sk': 6, 'cs_bill_addr_sk': 7, 'cs_ship_customer_sk': 8, 'cs_ship_cdemo_sk': 9, 'cs_ship_hdemo_sk': 10, 'cs_ship_addr_sk': 11, 'cs_call_center_sk': 12, 'cs_catalog_page_sk': 13, 'cs_ship_mode_sk': 14, 'cs_warehouse_sk': 15, 'cs_item_sk': 16, 'cs_promo_sk': 17, 'cs_order_number': 18, 'cs_quantity': 19, 'cs_wholesale_cost': 20, 'cs_list_price': 21, 'cs_sales_price': 22, 'cs_ext_discount_amt': 23, 'cs_ext_sales_price': 24, 'cs_ext_wholesale_cost': 25, 'cs_ext_list_price': 26, 'cs_ext_tax': 27, 'cs_coupon_amt': 28, 'cs_ext_ship_cost': 29, 'cs_net_paid': 30, 'cs_net_paid_inc_tax': 31, 'cs_net_paid_inc_ship': 32, 'cs_net_paid_inc_ship_tax': 33, 'cs_net_profit': 34},
                      'customer_address': {'ca_address_sk': 1, 'ca_address_id': 2, 'ca_street_number': 3, 'ca_street_name': 4, 'ca_street_type': 5, 'ca_suite_number': 6, 'ca_city': 7, 'ca_county': 8, 'ca_state': 9, 'ca_zip': 10, 'ca_country': 11, 'ca_gmt_offset': 12, 'ca_location_type': 13},
                      'customer': {'c_customer_sk': 1, 'c_customer_id': 2, 'c_current_cdemo_sk': 3, 'c_current_hdemo_sk': 4, 'c_current_addr_sk': 5, 'c_first_shipto_date_sk': 6, 'c_first_sales_date_sk': 7, 'c_salutation': 8, 'c_first_name': 9, 'c_last_name': 10, 'c_preferred_cust_flag': 11, 'c_birth_day': 12, 'c_birth_month': 13, 'c_birth_year': 14, 'c_birth_country': 15, 'c_login': 16, 'c_email_address': 17, 'c_last_review_date_sk': 18},
                      'customer_demographics': {'cd_demo_sk': 1, 'cd_gender': 2, 'cd_marital_status': 3, 'cd_education_status': 4, 'cd_purchase_estimate': 5, 'cd_credit_rating': 6, 'cd_dep_count': 7, 'cd_dep_employed_count': 8, 'cd_dep_college_count': 9},
                      'date_dim': {'d_date_sk': 1, 'd_date_id': 2, 'd_date': 3, 'd_month_seq': 4, 'd_week_seq': 5, 'd_quarter_seq': 6, 'd_year': 7, 'd_dow': 8, 'd_moy': 9, 'd_dom': 10, 'd_qoy': 11, 'd_fy_year': 12, 'd_fy_quarter_seq': 13, 'd_fy_week_seq': 14, 'd_day_name': 15, 'd_quarter_name': 16, 'd_holiday': 17, 'd_weekend': 18, 'd_following_holiday': 19, 'd_first_dom': 20, 'd_last_dom': 21, 'd_same_day_ly': 22, 'd_same_day_lq': 23, 'd_current_day': 24, 'd_current_week': 25, 'd_current_month': 26, 'd_current_quarter': 27, 'd_current_year': 28},
                      'dbgen_version': {'dv_version': 1, 'dv_create_date': 2, 'dv_create_time': 3, 'dv_cmdline_args': 4},
                      'household_demographics': {'hd_demo_sk': 1, 'hd_income_band_sk': 2, 'hd_buy_potential': 3, 'hd_dep_count': 4, 'hd_vehicle_count': 5},
                      'income_band': {'ib_income_band_sk': 1, 'ib_lower_bound': 2, 'ib_upper_bound': 3},
                      'inventory': {'inv_date_sk': 1, 'inv_item_sk': 2, 'inv_warehouse_sk': 3, 'inv_quantity_on_hand': 4},
                      'item': {'i_item_sk': 1, 'i_item_id': 2, 'i_rec_start_date': 3, 'i_rec_end_date': 4, 'i_item_desc': 5, 'i_current_price': 6, 'i_wholesale_cost': 7, 'i_brand_id': 8, 'i_brand': 9, 'i_class_id': 10, 'i_class': 11, 'i_category_id': 12, 'i_category': 13, 'i_manufact_id': 14, 'i_manufact': 15, 'i_size': 16, 'i_formulation': 17, 'i_color': 18, 'i_units': 19, 'i_container': 20, 'i_manager_id': 21, 'i_product_name': 22},
                      'promotion': {'p_promo_sk': 1, 'p_promo_id': 2, 'p_start_date_sk': 3, 'p_end_date_sk': 4, 'p_item_sk': 5, 'p_cost': 6, 'p_response_target': 7, 'p_promo_name': 8, 'p_channel_dmail': 9, 'p_channel_email': 10, 'p_channel_catalog': 11, 'p_channel_tv': 12, 'p_channel_radio': 13, 'p_channel_press': 14, 'p_channel_event': 15, 'p_channel_demo': 16, 'p_channel_details': 17, 'p_purpose': 18, 'p_discount_active': 19},
                      'reason': {'r_reason_sk': 1, 'r_reason_id': 2, 'r_reason_desc': 3},
                      'ship_mode': {'sm_ship_mode_sk': 1, 'sm_ship_mode_id': 2, 'sm_type': 3, 'sm_code': 4, 'sm_carrier': 5, 'sm_contract': 6},
                      'store': {'s_store_sk': 1, 's_store_id': 2, 's_rec_start_date': 3, 's_rec_end_date': 4, 's_closed_date_sk': 5, 's_store_name': 6, 's_number_employees': 7, 's_floor_space': 8, 's_hours': 9, 's_manager': 10, 's_market_id': 11, 's_geography_class': 12, 's_market_desc': 13, 's_market_manager': 14, 's_division_id': 15, 's_division_name': 16, 's_company_id': 17, 's_company_name': 18, 's_street_number': 19, 's_street_name': 20, 's_street_type': 21, 's_suite_number': 22, 's_city': 23, 's_county': 24, 's_state': 25, 's_zip': 26, 's_country': 27, 's_gmt_offset': 28, 's_tax_precentage': 29},
                      'store_returns': {'sr_returned_date_sk': 1, 'sr_return_time_sk': 2, 'sr_item_sk': 3, 'sr_customer_sk': 4, 'sr_cdemo_sk': 5, 'sr_hdemo_sk': 6, 'sr_addr_sk': 7, 'sr_store_sk': 8, 'sr_reason_sk': 9, 'sr_ticket_number': 10, 'sr_return_quantity': 11, 'sr_return_amt': 12, 'sr_return_tax': 13, 'sr_return_amt_inc_tax': 14, 'sr_fee': 15, 'sr_return_ship_cost': 16, 'sr_refunded_cash': 17, 'sr_reversed_charge': 18, 'sr_store_credit': 19, 'sr_net_loss': 20},
                      'store_sales': {'ss_sold_date_sk': 1, 'ss_sold_time_sk': 2, 'ss_item_sk': 3, 'ss_customer_sk': 4, 'ss_cdemo_sk': 5, 'ss_hdemo_sk': 6, 'ss_addr_sk': 7, 'ss_store_sk': 8, 'ss_promo_sk': 9, 'ss_ticket_number': 10, 'ss_quantity': 11, 'ss_wholesale_cost': 12, 'ss_list_price': 13, 'ss_sales_price': 14, 'ss_ext_discount_amt': 15, 'ss_ext_sales_price': 16, 'ss_ext_wholesale_cost': 17, 'ss_ext_list_price': 18, 'ss_ext_tax': 19, 'ss_coupon_amt': 20, 'ss_net_paid': 21, 'ss_net_paid_inc_tax': 22, 'ss_net_profit': 23},
                      'time_dim': {'t_time_sk': 1, 't_time_id': 2, 't_time': 3, 't_hour': 4, 't_minute': 5, 't_second': 6, 't_am_pm': 7, 't_shift': 8, 't_sub_shift': 9, 't_meal_time': 10},
                      'warehouse': {'w_warehouse_sk': 1, 'w_warehouse_id': 2, 'w_warehouse_name': 3, 'w_warehouse_sq_ft': 4, 'w_street_number': 5, 'w_street_name': 6, 'w_street_type': 7, 'w_suite_number': 8, 'w_city': 9, 'w_county': 10, 'w_state': 11, 'w_zip': 12, 'w_country': 13, 'w_gmt_offset': 14},
                      'web_page': {'wp_web_page_sk': 1, 'wp_web_page_id': 2, 'wp_rec_start_date': 3, 'wp_rec_end_date': 4, 'wp_creation_date_sk': 5, 'wp_access_date_sk': 6, 'wp_autogen_flag': 7, 'wp_customer_sk': 8, 'wp_url': 9, 'wp_type': 10, 'wp_char_count': 11, 'wp_link_count': 12, 'wp_image_count': 13, 'wp_max_ad_count': 14},
                      'web_returns': {'wr_returned_date_sk': 1, 'wr_returned_time_sk': 2, 'wr_item_sk': 3, 'wr_refunded_customer_sk': 4, 'wr_refunded_cdemo_sk': 5, 'wr_refunded_hdemo_sk': 6, 'wr_refunded_addr_sk': 7, 'wr_returning_customer_sk': 8, 'wr_returning_cdemo_sk': 9, 'wr_returning_hdemo_sk': 10, 'wr_returning_addr_sk': 11, 'wr_web_page_sk': 12, 'wr_reason_sk': 13, 'wr_order_number': 14, 'wr_return_quantity': 15, 'wr_return_amt': 16, 'wr_return_tax': 17, 'wr_return_amt_inc_tax': 18, 'wr_fee': 19, 'wr_return_ship_cost': 20, 'wr_refunded_cash': 21, 'wr_reversed_charge': 22, 'wr_account_credit': 23, 'wr_net_loss': 24},
                      'web_sales': {'ws_sold_date_sk': 1, 'ws_sold_time_sk': 2, 'ws_ship_date_sk': 3, 'ws_item_sk': 4, 'ws_bill_customer_sk': 5, 'ws_bill_cdemo_sk': 6, 'ws_bill_hdemo_sk': 7, 'ws_bill_addr_sk': 8, 'ws_ship_customer_sk': 9, 'ws_ship_cdemo_sk': 10, 'ws_ship_hdemo_sk': 11, 'ws_ship_addr_sk': 12, 'ws_web_page_sk': 13, 'ws_web_site_sk': 14, 'ws_ship_mode_sk': 15, 'ws_warehouse_sk': 16, 'ws_promo_sk': 17, 'ws_order_number': 18, 'ws_quantity': 19, 'ws_wholesale_cost': 20, 'ws_list_price': 21, 'ws_sales_price': 22, 'ws_ext_discount_amt': 23, 'ws_ext_sales_price': 24, 'ws_ext_wholesale_cost': 25, 'ws_ext_list_price': 26, 'ws_ext_tax': 27, 'ws_coupon_amt': 28, 'ws_ext_ship_cost': 29, 'ws_net_paid': 30, 'ws_net_paid_inc_tax': 31, 'ws_net_paid_inc_ship': 32, 'ws_net_paid_inc_ship_tax': 33, 'ws_net_profit': 34},
                      'web_site': {'web_site_sk': 1, 'web_site_id': 2, 'web_rec_start_date': 3, 'web_rec_end_date': 4, 'web_name': 5, 'web_open_date_sk': 6, 'web_close_date_sk': 7, 'web_class': 8, 'web_manager': 9, 'web_mkt_id': 10, 'web_mkt_class': 11, 'web_mkt_desc': 12, 'web_market_manager': 13, 'web_company_id': 14, 'web_company_name': 15, 'web_street_number': 16, 'web_street_name': 17, 'web_street_type': 18, 'web_suite_number': 19, 'web_city': 20, 'web_county': 21, 'web_state': 22, 'web_zip': 23, 'web_country': 24, 'web_gmt_offset': 25, 'web_tax_percentage': 26}}
    join_pairs_list = find_join_pair()
    data = [] #<t1, c1, t2, c2>
    for join in join_pairs_list:
        try:
            t1 = attribute_to_table(join[0])
            t2 = attribute_to_table(join[1])
            # cur_data = [table_dict[t1], tables_columns[t1][join[0]], table_dict[t2], tables_columns[t2][join[1]]]
            cur_data = [table_dict[t1], table_dict[t2]]
            data.append(cur_data)
            cur_data = [table_dict[t2], table_dict[t1]]
            data.append(cur_data)
        except TypeError as e:
            # print(e)
            print("wow this is an exception !")

    print(data)








def find_join_pair():
    folder_path = '/home/postgres/tpc/tpcds/queries'
    join_pairs = []  # 原始连接对列表
    for filename in os.listdir(folder_path):
        # if filename not in file_name_list:
        #     continue
        query_path = os.path.join(folder_path, filename)
        sql = ""
        with open(query_path, "r") as f:
            for line in f:
                sql = sql + line
        matches = re.findall(r'\b(\w+)\s*=\s*(\w+)\b', sql)
        for match in matches:
            key, value = match
            if not key.isdigit() and not value.isdigit():
                join_pairs.append(match)
    # counter = Counter([tuple(sorted(pair)) for pair in join_pairs])
    # join_pairs_frequency = [(pair[0], pair[1], count) for pair, count in counter.items()]
    # for i in join_pairs_frequency:
    #     print(i)
    # return join_pairs_frequency
    # print(join_pairs[2])
    return join_pairs


if __name__ == '__main__':
    coder()
    # find_join_pair()

