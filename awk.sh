# 生成 1000 个真实度极高的域名记录，复制 10 份并彻底打乱
printf "%s\n" {www,api,static,img,cdn,m,mail,blog,v1,v2}.{google.com,youtube.com,facebook.com,baidu.com,qq.com,taobao.com,tmall.com,github.com,microsoft.com,apple.com,amazon.com,twitter.com,wikipedia.org,jd.com,weibo.com,alipay.com,netflix.com,bing.com,bilibili.com,zhihu.com,cloudflare.com,openai.com,icloud.com,sohu.com,sina.com.cn,163.com,toutiao.com,meituan.com,douyin.com,csdn.net,pinduoduo.com,tiktok.com} | \
shuf -n 1000 | awk '{print $1 " A"}' > base.tmp && \
for i in {1..10}; do cat base.tmp; done | shuf > final_queries.txt && \
rm base.tmp && echo "生成成功: final_queries.txt (10,000 条记录)"
