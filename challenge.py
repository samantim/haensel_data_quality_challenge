import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns


def connect_db(db_path : str) -> sqlite3.Connection:
    # Create a connection to run the queries
    con = sqlite3.connect(db_path)
    return con


def question1(con : sqlite3.Connection):
    # Question1 : "Are the costs in the 'api_adwords_costs' table fully covered in the 'session_sources' table? Any campaigns where you see issues?"
    plus_minus = 0.1
    # This query join 'api_adwords_costs' and 'session_sources' tables based on 'event_date' and 'campaign_id'
    # It is important to know combination of event_date and campaign_id columns make an unique index in 'api_adwords_costs' table
    # Since for every 'event_date' and 'campaign_id' in 'api_adwords_costs' table there are several sessions and cpc s in the 'session_sources' table, we have to calculate the average of their cpc to multiply to clicks
    query = """select api_adwords_costs.event_date,api_adwords_costs.campaign_id, sum(distinct api_adwords_costs.cost) as cost_from_api_adwords_costs, ROUND(cast(coalesce(avg(session_sources.cpc),0) as numeric)*sum(distinct api_adwords_costs.clicks), 3) as cost_from_session_sources
                , ROUND(cast(coalesce(avg(session_sources.cpc),0) as numeric)*sum(distinct api_adwords_costs.clicks), 3) - sum(distinct api_adwords_costs.cost) as difference
                from api_adwords_costs left join session_sources on session_sources.event_date = api_adwords_costs.event_date and session_sources.campaign_id = api_adwords_costs.campaign_id
                group by api_adwords_costs.event_date,api_adwords_costs.campaign_id"""

    data = pd.read_sql(sql=query,con=con)
    print(f"There are {data.shape[0]} rows for comparison")

    # we decide to exhibit records with more than 10% difference from the 'api_adwords_costs' cost
    data = pd.read_sql(sql=query + f" having ROUND(cast(coalesce(avg(session_sources.cpc),0) as numeric)*sum(distinct api_adwords_costs.clicks), 3) not between sum(distinct api_adwords_costs.cost)*{1-plus_minus} and sum(distinct api_adwords_costs.cost)*{1+plus_minus}""",con=con)
    print(f"There are {data.shape[0]} rows with more than {plus_minus*100}% difference")

    fig = plt.figure(figsize=(16, 9), dpi=300)
    # Get all data from the query
    data = pd.read_sql(sql=query,con=con)

    # Group data by event_date
    data_groupby = data.groupby(by=["event_date"]).sum()
    # Only show the day for better visualization
    data_groupby.index = data_groupby.index.str[-2:]
    # Plot lines of cost_from_api_adwords_costs and cost_from_session_sources based on event_date
    sns.lineplot(data_groupby, x="event_date", y="cost_from_api_adwords_costs")
    sns.lineplot(data_groupby, x="event_date", y="cost_from_session_sources")
    plt.title("Cost comparison based on event_date")
    plt.ylabel("costs")
    # Create proper legends
    zero_patch = mpatches.Patch(color = "blue", label="cost_from_api_adwords_costs")
    one_patch = mpatches.Patch(color = "orange", label="cost_from_session_sources")
    plt.legend(handles=[zero_patch, one_patch], loc="upper right")
    plt.tight_layout(pad=1, h_pad=0.5, w_pad=0.5) 
    # Save the image file
    plt.savefig(fname=f"output/cost_comparison_based_on_event_date.png", format="png", dpi = fig.dpi)  

    # Print 5 records from +/- sides which have the most difference 
    sorted_date = data_groupby.sort_values(["difference"],ascending=False)
    print(f"Cost comparison based on event_date:\n{sorted_date.head(5)}")
    print(f"Cost comparison based on event_date:\n{sorted_date.tail(5)}")

    # Clear the plot canvas
    plt.clf()

    # Group data by campaign_id
    data_groupby = data.groupby(by=["campaign_id"]).sum()
    # Plot lines of cost_from_api_adwords_costs and cost_from_session_sources based on campaign_id
    sns.lineplot(data_groupby, x="campaign_id", y="cost_from_api_adwords_costs")
    sns.lineplot(data_groupby, x="campaign_id", y="cost_from_session_sources")
    plt.title("Cost comparison based on campaign_id")
    plt.ylabel("costs")
    # Create proper legends
    zero_patch = mpatches.Patch(color = "blue", label="cost_from_api_adwords_costs")
    one_patch = mpatches.Patch(color = "orange", label="cost_from_session_sources")
    plt.legend(handles=[zero_patch, one_patch], loc="upper right")
    plt.tight_layout(pad=1, h_pad=0.5, w_pad=0.5)
    # Save the image file
    plt.savefig(fname=f"output/cost_comparison_based_on_campaign_id.png", format="png", dpi = fig.dpi)  

    # Print 5 records from +/- sides which have the most difference 
    sorted_date = data_groupby.sort_values(["difference"],ascending=False)
    print(f"Cost comparison based on campaign_id:\n{sorted_date.head(5)}")
    print(f"Cost comparison based on campaign_id:\n{sorted_date.tail(5)}")


def question2(con : sqlite3.Connection):
    # Are the conversions in the 'conversions' table stable over time? Any pattern?
    # This query returns sum of revenue for each conversion date
    query = """select conv_date, sum(revenue) as sum_revenue
                from conversions
                group by conv_date
                order by conv_date"""
    
    fig = plt.figure(figsize=(16, 9), dpi=300)
    # Get all data from the query
    data = pd.read_sql(sql=query,con=con)
    # Only show the day for better visualization
    data["conv_date"] = data["conv_date"].str[-2:]

    # Plot the line of sum of revenue changes based over time
    sns.lineplot(data, x="conv_date", y="sum_revenue")
    plt.title("conversions over time")
    plt.ylabel("sum of revenue")
    plt.tight_layout(pad=1, h_pad=0.5, w_pad=0.5) 
    plt.savefig(fname=f"output/conversions_over_time.png", format="png", dpi = fig.dpi)  


def question3(con : sqlite3.Connection):
    # Double check conversions ('conversions' table) with backend ('conversions_backend' table), any issues?
    # This query returns the comparison between sum of conversion from conversions and backend based on conv_date
    query_conv_date = """select conversions.conv_date as conv_date, sum(conversions.revenue) as conversions_sum_revenue, sum(conversions_backend.revenue) as conversions_backend_sum_revenue
                        from conversions inner join conversions_backend on conversions.conv_id = conversions_backend.conv_id
                        group by conversions.conv_date
                        order by conv_date"""
    
    fig = plt.figure(figsize=(16, 9), dpi=300)
    # Get all data from the query
    data = pd.read_sql(sql=query_conv_date,con=con)
    # Only show the day for better visualization
    data["conv_date"] = data["conv_date"].str[-2:]

    # Bar plot to campare revenues
    data.plot.bar(x="conv_date", y=["conversions_sum_revenue", "conversions_backend_sum_revenue"])
    plt.title("conversions over time")
    plt.tight_layout(pad=1, h_pad=0.5, w_pad=0.5) 
    plt.savefig(fname=f"output/conversions_vs_backend_conv_date.png", format="png", dpi = fig.dpi)  

    # This query returns the comparison between sum of conversion from conversions and backend based on market
    query_market = """select conversions.market as market, sum(conversions.revenue) as conversions_sum_revenue, sum(conversions_backend.revenue) as conversions_backend_sum_revenue
                        from conversions inner join conversions_backend on conversions.conv_id = conversions_backend.conv_id
                        group by conversions.market
                        order by market"""
    
    fig = plt.figure(figsize=(16, 9), dpi=300)
    # Get all data from the query
    data = pd.read_sql(sql=query_market,con=con)

    # Bar plot to campare revenues
    data.plot.bar(x="market", y=["conversions_sum_revenue", "conversions_backend_sum_revenue"])
    plt.title("conversions over market")
    plt.tight_layout(pad=1, h_pad=0.5, w_pad=0.5) 
    plt.savefig(fname=f"output/conversions_vs_backend_market.png", format="png", dpi = fig.dpi)  

    # This query returns the differences between to tables based on conv_date order by difference amount
    query_conv_date = """select conv_date, sum(conversions_backend_revenue) as conversions_backend_sum_revenue
                        from(select conversions.conv_date as conv_date, conversions.market as market, conversions.revenue as conversions_revenue, conversions_backend.revenue as conversions_backend_revenue
                        from conversions cross join conversions_backend on conversions.conv_id = conversions_backend.conv_id
                        where conversions.revenue <> conversions_backend.revenue) as q
                        group by conv_date
                        order by conversions_backend_sum_revenue desc"""
    
    fig = plt.figure(figsize=(16, 9), dpi=300)
    # Get all data from the query
    data = pd.read_sql(sql=query_conv_date,con=con)
    # Only show the day for better visualization
    data["conv_date"] = data["conv_date"].str[-2:]

     # Bar plot to show the amount of differences
    data.plot.bar(x="conv_date", y=["conversions_backend_sum_revenue"])
    plt.title("conversions differences based on time")
    plt.tight_layout(pad=1, h_pad=0.5, w_pad=0.5) 
    plt.savefig(fname=f"output/conversions_differences_conv_date.png", format="png", dpi = fig.dpi)  

    # This query returns the differences between to tables based on market order by difference amount
    query_market = """select market, sum(conversions_backend_revenue) as conversions_backend_sum_revenue
                    from(select conversions.conv_date as conv_date, conversions.market as market, conversions.revenue as conversions_revenue, conversions_backend.revenue as conversions_backend_revenue
                    from conversions cross join conversions_backend on conversions.conv_id = conversions_backend.conv_id
                    where conversions.revenue <> conversions_backend.revenue) as q
                    group by market
                    order by conversions_backend_sum_revenue desc"""

    fig = plt.figure(figsize=(16, 9), dpi=300)
    # Get all data from the query
    data = pd.read_sql(sql=query_market,con=con)

    # Bar plot to show the amount of differences
    data.plot.bar(x="market", y=["conversions_backend_sum_revenue"])
    plt.title("conversions differences based on market")
    plt.tight_layout(pad=1, h_pad=0.5, w_pad=0.5) 
    plt.savefig(fname=f"output/conversions_differences_market.png", format="png", dpi = fig.dpi)  


def question4(con : sqlite3.Connection):
    # Are attribution results consistent? Do you find any conversions where the 'ihc' values don't make sense?
    # This query returns the sum of ihc for every conv_id which should be 1. The results with value not equal to 1 are considered as inconsistency
    query = """select conv_id, round(sum(ihc),2) as sum_ihc
                from attribution_customer_journey
                group by conv_id
                having round(sum(ihc)) <> 1
				order by sum_ihc"""
    
    # Get all data from the query
    data = pd.read_sql(sql=query,con=con)
    print(f"Attributions with inconsistent ihc sum:\n{data}")

def question5_subplot(df : pd.DataFrame, sub_index : int, channel_name : str):
    # This function is responsible to create the subplot and prevents code redundancy
    plt.subplot(5,1,sub_index)
    # Only show the day for better visualization
    df["event_date"] = df["event_date"].str[-2:]
    sns.lineplot(df, x="event_date", y="session_count")
    plt.title(channel_name)
    plt.xlabel("")
    plt.ylabel("")  

def question5(con : sqlite3.Connection):
    # (Bonus) Do we have an issue with channeling? Are the number of sessions per channel stable over time?
    # This query returns the count of sessions based on channel_name and event_date
    # Since there are some duplicate channel names which present in the table with different names, they are unified with case when block
    query = """select channel_name , event_date, coalesce(count(session_id),0) as session_count
                from(select case when channel_name = "Affiliates" then "Affiliate" 
                when channel_name = "Direct" then "Direct Traffic" 
                when channel_name = "Display" then "Display Remarketing" 
                when channel_name = "SEA - Brand" then "SEA - Branded" 
                when channel_name = "SEA - Non-Brand" then "SEA - Non-branded" 
                when channel_name = "Shopping - Brand" then "Shopping - Branded" 
                when channel_name = "Shopping - Non Brand" then "Shopping - Non-branded" 
                when channel_name = "Social Organic" then "Social - Organic" 
                when channel_name = "Social Paid" then "Social - Paid" 
                when channel_name = "Social Paid" then "Social - Paid" 
                else channel_name end as channel_name , event_date, session_id
                from session_sources) as q
                group by channel_name, event_date"""

    data = pd.read_sql(query,con=con)
    data.sort_values(["channel_name", "event_date"])

    # This code block is responsible for creating subplot for each channel_name, and shows the number of sessions over time
    i = 0
    sub_index = 1
    df = pd.DataFrame()
    channel_name = data.loc[i,"channel_name"]
    fig = plt.figure(figsize=(16, 9), dpi=300)
    # Loop over all records
    while i < data.shape[0] :
        if channel_name != data.loc[i,"channel_name"]:
            # Create a subplot for the channel_name
            question5_subplot(df, sub_index, channel_name)
            if sub_index % 5 == 0:
                plt.tight_layout(pad=1, h_pad=0.5, w_pad=0.5) 
                # Save the sob plot to a file
                plt.savefig(fname=f"output/channel_session_count{i}.png", format="png", dpi = fig.dpi)
                sub_index = 0
                plt.clf()
            sub_index += 1
            # Clear the dataframe
            df.drop(axis="index", index=df.index, inplace=True) 

        # add the record to a new dataframe
        channel_name = data.loc[i,"channel_name"]
        row = df.shape[0]
        df.loc[row,"event_date"] = data.loc[i,"event_date"]
        df.loc[row,"session_count"] = data.loc[i,"session_count"]

        i += 1

    # Create a subplot for the channel_name
    question5_subplot(df, sub_index, channel_name)
    sub_index += 1
    df.drop(axis="index", index=df.index, inplace=True) 

    plt.tight_layout(pad=1, h_pad=0.5, w_pad=0.5) 
    # Save the sob plot to a file
    plt.savefig(fname=f"output/channel_session_count{i}.png", format="png", dpi = fig.dpi)


def main():
    con = connect_db("challenge.db")
    question1(con=con)
    question2(con=con)
    question3(con=con)
    question4(con=con)
    question5(con=con)

if __name__ == "__main__":
    main()