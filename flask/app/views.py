from flask import jsonify
from flask import render_template
from app import app
from flask import request
from cassandra.cluster import Cluster
from datetime import timedelta
from datetime import datetime
import json



cluster = Cluster(['ec2-52-34-145-96.us-west-2.compute.amazonaws.com'])

session = cluster.connect('transaction_space')

@app.route('/')
def get_card_holder():
    return render_template("card_holder.html")

@app.route("/", methods=['POST'])
def get_card_holder_info(chart_type = 'column', chart_height = 350):
    userid = request.form["userid"]
    #userid = 'Liam B110'

    #determine total amount spent today up to now from the transaction_log table
    current_date=datetime.today().strftime("%Y-%m-%d")
    stmt="SELECT * FROM transaction_log WHERE name=%s AND date=%s"
    response=session.execute(stmt, parameters=[userid,current_date])
    response_list=[]
    for val in response:
      response_list.append(val)
    #sum all the transactions
    sum_today=0
    for x in response_list:
       sum_today=sum_today + x.transaction
    
    #compare with the daily transaction with the past 7 days
    delta=timedelta(days=7)
    date_7days_ago=(datetime.today()-delta).strftime("%Y-%m-%d")

    stmt = "SELECT * FROM  daily_summary WHERE name=%s AND date>=%s  AND date < %s"
    response=session.execute(stmt, parameters=[userid,date_7days_ago, current_date])
    response_list1=[]
    for val in response:
       response_list1.append(val)
    
    #add today's total to jsonresponse_daily since today's transaction has not been batch processed yet
    jsonresponse_daily=[[current_date, sum_today]]
    
    def prev_date(d):
       return d-timedelta(days=1)

    date_filler=prev_date(datetime.today())
    #fill in transaction amount of dates not found in Cassandra to 0    
    for x in response_list1:
       while x.date<date_filler.strftime("%Y-%m-%d"):
          jsonresponse_daily.insert(0,[date_filler.strftime("%Y-%m-%d"),0])
          date_filler=prev_date(date_filler)
       jsonresponse_daily.insert(0,[str(x.date), float(x.total_transaction)])
       date_filler=prev_date(date_filler)
   
    while date_filler.strftime("%Y-%m-%d")>= date_7days_ago:
       jsonresponse_daily.insert(0,[date_filler.strftime("%Y-%m-%d"),0])
       date_filler=prev_date(date_filler)


    series1=jsonresponse_daily
    chartID1='chart_ID1'
    chart1 = {"renderTo":chartID1 , "type": chart_type, "height": chart_height,}
    title1 = {"text": "Comparison with Past 7 Days' Transaction", "style": {"fontWeight":'bold'}}
    xAxis1 = {"categories": ['Date']}
    yAxis1 = {"title": {"text": 'Daily Total Transactions ($)'}}
    
    #determine this month's total (up to now)
    current_month=datetime.today().strftime("%Y-%m")
    stmt= "SELECT * FROM monthly_summary WHERE name=%s AND month=%s"
    response=session.execute(stmt, parameters=[userid, current_month])
    sum_this_month= sum_today
    for val in response:
       sum_this_month=sum_this_month+val.total_transaction
    
    #determine an appropriate message
    if sum_this_month<1000:
        msg="You are doing great! Way below your targeted budget of $2000 for this month!"
    elif sum_this_month<1700:
        msg="Keep it up and you will make it below your targeted budget of $2000 for this month!"
    elif sum_this_month<2000:
        msg="Easy on the spending there!  You are about to exceed your targeted budget of $2000 for this month!"
    else:
        msg="Uh-oh, you have spent more than your planned budget of $2000 for this month!!!"

    #determine the total transaction in each of the past three months
    delta=timedelta(days=90)
    date_3months_ago=(datetime.today()-delta).strftime("%Y-%m")
    stmt = "SELECT * FROM monthly_summary WHERE name=%s AND month>=%s AND month<%s"
    response=session.execute(stmt, parameters=[userid, date_3months_ago,current_month])
    response_list2=[]
    for val in response:
      response_list2.append(val)
    
    #fill in transaction of month not found in Cassandra to 0 
    def prev_month(d):
       return d-timedelta(days=31)
    jsonresponse_monthly=[[current_month,sum_this_month]]
    month_filler=prev_month(datetime.today())
    for x in response_list2:  
       while x.month<month_filler.strftime("%Y-%m"):
          jsonresponse_monthly.insert(0, [month_filler.strftime("%Y-%m"),0])
          month_filler=prev_month(month_filler)
       jsonresponse_monthly.insert(0, [str(x.month), float(x.total_transaction)])
       month_filler=prev_month(month_filler)
    while month_filler.strftime("%Y-%m")>=date_3months_ago:
       jsonresponse_monthly.insert(0,[month_filler.strftime("%Y-%m"),0])
       month_filler=prev_month(month_filler)
   
    series2=jsonresponse_monthly
    chartID2='chart_ID2'
    chart2 = {"renderTo": chartID2, "type": chart_type, "height": chart_height,}
    title2 = {"text": "Comparison with Previous 3 Months' Total Transactions", "style": {"fontWeight":'bold'}}
    xAxis2 = {"categories": ['Months']}
    yAxis2 = {"title": {"text": 'Monthly Total Transactions ($)'}}

    #determine the total transaction in each of the past five "same day of the week"
    wkday=datetime.now().weekday()+1
    delta=timedelta(days=35)
    date_5wks_ago=(datetime.now()-delta).strftime("%Y-%m-%d")
    stmt = "SELECT date, total_transaction FROM day_of_week_summary WHERE name=%s and day_of_week=%s and date >=%s and date <%s"
    response=session.execute(stmt, parameters=[userid,str(wkday),date_5wks_ago, current_date])
    response_list3=[]
    for val in response:
      response_list3.append(val)
    
    #add today's total to jsonresponse_wkday since today's transaction has not been batch processed yet
    jsonresponse_wkday=[[current_date, sum_today]]
    
    #fill in the transaction amount of dates not found in Cassandra to 0
    def prev_week(d):
       return d-timedelta(days=7)

    date_filler=prev_week(datetime.today())
    
    for x in response_list3:
       while x.date<date_filler.strftime("%Y-%m-%d"):
          jsonresponse_wkday.insert(0,[date_filler.strftime("%Y-%m-%d"),0])
          date_filler=prev_week(date_filler)
       jsonresponse_wkday.insert(0,[str(x.date), float(x.total_transaction)])
       date_filler=prev_week(date_filler)

    while date_filler.strftime("%Y-%m-%d")>date_5wks_ago:
       jsonresponse_wkday.insert(0,[date_filler.strftime("%Y-%m-%d"),0])
       date_filler=prev_week(date_filler)
 
    series3=jsonresponse_wkday
    chartID3='chart_ID3'
    chart3 = {"renderTo": chartID3, "type": chart_type, "height": chart_height,}
    def numbers_to_weekdays(argument):
      switcher = {
        1: "Monday",
        2: "Tuesday",
        3: "Wednesday",
        4: "Thursday",
        5: "Friday",
        6: "Saturday",
        7: "Sunday"
      }
      return switcher.get(argument, "nothing")
    title3 = {"text": 'Comparison with Previous 5 '+ numbers_to_weekdays(wkday)+"'s Transactions", "style": {"fontWeight":'bold'}}
    xAxis3 = {"categories": ['Dates']}
    yAxis3 = {"title": {"text": 'Daily Total Transactions ($)'}}

    #determine the transaction amount of each category for the current month
    current_month=datetime.today().strftime("%Y-%m")

    stmt = "SELECT * FROM  user_category_summary WHERE name=%s AND month=%s" 
    response=session.execute(stmt, parameters=[userid,current_month]) 
    response_list4=[] 
 
    for val in response: 
      response_list4.append(val) 
    
    #update category transaction by today's transaction which has not been batch processed
    jsondata=[]
    for x in response_list4:
       x_trans_type=str(x.trans_type)
       x_total_transaction=x.total_transaction
       for y in response_list:
          if x_trans_type==y.trans_type:
              x_total_transaction=x_total_transaction+y.transaction
       jsondata.append({"name":x_trans_type, "y":float(x_total_transaction)})
    #add in new categories that accrued today
    for x in response_list:
       x_in_y=0
       for y in response_list4:
          if x.trans_type==y.trans_type:
              x_in_y=1
       if x_in_y == 0:
          jsondata.append({"name": str(x.trans_type), "y":float(x.transaction)})
        
    for i in range (0, len(jsondata)):
       for j in range (i, len(jsondata)):
           jsoni=jsondata[i]
           jsonj=jsondata[j]
           if jsoni["y"]<jsonj["y"]:
               temp=jsondata[i]
               jsondata[i]=jsondata[j]
               jsondata[j]=temp       
     
    series4=[{"name":'category',"data":jsondata}] 
    
    chartID4='chart_ID4' 
    chart4 = {"renderTo":chartID4 , "type": 'pie', "height": chart_height,} 
    title4 = {"text": 'Total Transaction by Category', "style": {"fontWeight":'bold'}} 
    xAxis4 = {"categories": ['Category']} 
    yAxis4 = {"title": {"text": 'Total Transactions ($)'}} 
 
    response_list5=[] 
    
    #determine the top 5 transaction categories of the user and compare the amount with that of an average buyer for the current month 
    for i in range(0,5): 
      if len(jsondata)<=i: 
        break 
      response_list5.append(jsondata[i]) 
   
    jsonresponse_top5 = [{"name": x['name'], "data":[x['y']]} for x in response_list5] 
    
    series5=jsonresponse_top5 
    #determine the average transaction amount of all buyers for each category
    stmt = "SELECT * FROM  category_summary_all_users WHERE month=%s"
    response=session.execute(stmt, parameters=[current_month])
    response_list6=[]
    for val in response:
       response_list6.append(val)
    response_shared=[]
    for val1 in response_list5:
      print val1['name']
      for val2 in response_list6:
        if val1['name']==val2.trans_type:
           response_shared.append(val2)

    shared_category=[str(x.trans_type) for x in response_shared]

    user_data=[float(x['y']) for x in response_list5]

    avg_data=[float(x.total_transaction)/float(x.distinct_users) for x in response_shared]

    jsonresponse_top5=[{"name":str(userid),"data":user_data},{"name":'Average of all buyers',"data":avg_data}]
    series5=jsonresponse_top5
    chartID5='chart_ID5'
    chart5 = {"renderTo": chartID5, "type": 'bar', "height": chart_height,}
    title5 = {"text": 'Top 5 Spending Categories', "style": {"fontWeight":'bold'}}
    xAxis5 = {"categories": shared_category}
    yAxis5 = {"title": {"text": 'Total Transactions ($)'}}

    return render_template('card_holder_graphs.html', userid=userid, sum_today=sum_today, sum_this_month=sum_this_month, msg=msg, chartID1=chartID1, chart1=chart1, series1=series1, title1=title1, xAxis1=xAxis1, yAxis1=yAxis1, chartID2=chartID2, chart2=chart2,series2=series2,title2=title2,xAxis2=xAxis2,yAxis2=yAxis2, chartID3=chartID3,chart3=chart3,series3=series3,title3=title3,xAxis3=xAxis3,yAxis3=yAxis3, chartID4=chartID4, chart4=chart4, series4=series4, title4=title4,xAxis4=xAxis4, yAxis4=yAxis4, chartID5=chartID5,chart5=chart5, series5=series5,title5=title5,xAxis5=xAxis5,yAxis5=yAxis5)


@app.route("/card_company")
def get_all_card_holder(chartID = 'chart_ID', chart_type = 'column', chart_height = 350):
    #determine the total monthly transaction amount of all users since the beginning (Jan 1,2015)
    stmt = "SELECT * FROM  monthly_summary_all_users"
    response=session.execute(stmt, parameters=[])
    response_list1=[]

    for val in response:
      response_list1.insert(0,val)

    jsonresponse_total=[[str(x.month),float(x.total_transaction)] for x in response_list1]
    series1=jsonresponse_total

    chartID1='chart_ID1'
    chart1 = {"renderTo": chartID1, "type": 'column', "height": chart_height,}
    title1={"text": 'Total Monthly Transaction for All Credit Card Holders', "style": {"fontWeight":'bold'}}
    xAxis1={"categories":['Month']}
    yAxis1={"title": {"text": 'Total Monthly Transactions ($)'}}
    
    #determine the average monthly transaction amount of all users since the beginning (Jan 1, 2015)
    jsonresponse_avg=[[str(x.month),float(x.total_transaction)/int(x.distinct_users)] for x in response_list1]
    series2=jsonresponse_avg

    chartID2='chart_ID2'
    chart2 = {"renderTo": chartID2, "type": 'column', "height": chart_height,}
    title2={"text": 'Average Monthly Transaction for Credit Card Holders', "style": {"fontWeight":'bold'}}
    xAxis2={"categories":['Month']}
    yAxis2={"title": {"text": 'Average Monthly Transactions ($)'}}

    return render_template('card_company1.html', chartID1=chartID1, chart1=chart1, series1=series1, title1=title1, xAxis1=xAxis1, yAxis1=yAxis1, chartID2=chartID2,chart2=chart2,series2=series2,title2=title2,xAxis2=xAxis2,yAxis2=yAxis2)



@app.route("/card_company", methods=['POST'])
def get_percentile_info(chartID = 'chart_ID', chart_type = 'column', chart_height = 350):
    cat_id = request.form["trans_type"]

    #determine the total monthly transaction amount of all users since the beginning (Jan 1,2015)
    stmt = "SELECT * FROM  monthly_summary_all_users"
    response=session.execute(stmt, parameters=[])
    response_list1=[]

    for val in response:
      response_list1.insert(0,val)

    jsonresponse_total=[[str(x.month),float(x.total_transaction)] for x in response_list1]
    series1=jsonresponse_total

    chartID1='chart_ID1'
    chart1 = {"renderTo": chartID1, "type": 'column', "height": chart_height,}
    title1={"text": 'Total Monthly Transaction for All Credit Card Holders', "style": {"fontWeight":'bold'}}
    xAxis1={"categories":['Month']}
    yAxis1={"title": {"text": 'Total Monthly Transactions ($)'}}

    #determine the average monthly transaction amount of all users since the beginning (Jan 1,2015)
    jsonresponse_avg=[[str(x.month),float(x.total_transaction)/int(x.distinct_users)] for x in response_list1]
    series2=jsonresponse_avg

    chartID2='chart_ID2'
    chart2 = {"renderTo": chartID2, "type": 'column', "height": chart_height,}
    title2={"text": 'Average Monthly Transaction for Credit Card Holders', "style": {"fontWeight":'bold'}}
    xAxis2={"categories":['Month']}
    yAxis2={"title": {"text": 'Average Monthly Transactions ($)'}}

    stmt = "SELECT * FROM  quarterly_category_percentile WHERE category=%s"
    response=session.execute(stmt, parameters=[cat_id])
    response_list3=[]

    #determine the quarterly percentiles for the category selected
    for val in response:
      response_list3.insert(0,val)

    quarters=[str(x.quarter) for x in response_list3]
    twenty_fifth=[float(x.twenty_fifth) for x in response_list3]
    fiftieth=[float(x.fiftieth) for x in response_list3]
    seventy_fifth=[float(x.seventy_fifth) for x in response_list3]
    ninetieth=[float(x.ninetieth) for x in response_list3]

    jsonresponse_pct=[{"name":'25th',"data":twenty_fifth},{"name":'50th',"data":fiftieth},{"name":'75th',"data":seventy_fifth},{"name":'90th',"data":ninetieth}]

    series3=jsonresponse_pct
    chartID3='chart_ID3'
    chart3 = {"renderTo":chartID3 , "type": chart_type, "height": chart_height,}
    title3 = {"text": 'Percentile of Transaction Amount in the "'+(str(cat_id)).capitalize()+'" Category for Each Quarter (3 months period)', "style": {"fontWeight":'bold'}}
    xAxis3 = {"categories": quarters}
    yAxis3 = {"title": {"text": 'Daily Total Transactions ($)'}}

    return render_template('card_company2.html', chartID1=chartID1, chart1=chart1, series1=series1, title1=title1, xAxis1=xAxis1, yAxis1=yAxis1, chartID2=chartID2, chart2=chart2, series2=series2, title2=title2, xAxis2=xAxis2, yAxis2=yAxis2, chartID3=chartID3, chart3=chart3, series3=series3, title3=title3, xAxis3=xAxis3, yAxis3=yAxis3)
