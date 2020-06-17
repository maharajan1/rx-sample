#include <rxcpp/rx.hpp>

#include <string>
#include <vector>
#include <librdkafka/rdkafkacpp.h>
#include <thread>
#include <unordered_map>

#ifndef _WIN32
#include <sys/time.h>
#else
#include <windows.h> /* for GetLocalTime */
#endif

class ExampleRebalanceCb : public RdKafka::RebalanceCb {
private:
  static void part_list_print (const std::vector<RdKafka::TopicPartition*>&partitions){
    for (unsigned int i = 0 ; i < partitions.size() ; i++)
      std::cerr << partitions[i]->topic() <<
	"[" << partitions[i]->partition() << "], ";
    std::cerr << "\n";
  }

public:
  void rebalance_cb (RdKafka::KafkaConsumer *consumer,
		     RdKafka::ErrorCode err,
                     std::vector<RdKafka::TopicPartition*> &partitions) {
    std::cerr << "RebalanceCb: " << RdKafka::err2str(err) << ": ";

    part_list_print(partitions);

    if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
      consumer->assign(partitions);
      m_partition_cnt = (int)partitions.size();
    } else {
      consumer->unassign();
      m_partition_cnt = 0;
    }
    m_eof_cnt = 0;
  }

  int m_partition_cnt = 0;
  int m_eof_cnt = 0;
};

class ExampleEventCb : public RdKafka::EventCb {
 public:
  void event_cb (RdKafka::Event &event) {

    print_time();

    switch (event.type())
    {
      case RdKafka::Event::EVENT_ERROR:
        if (event.fatal()) {
          std::cerr << "FATAL ";
        }
        std::cerr << "ERROR (" << RdKafka::err2str(event.err()) << "): " <<
            event.str() << std::endl;
        break;

      case RdKafka::Event::EVENT_STATS:
        std::cerr << "\"STATS\": " << event.str() << std::endl;
        break;

      case RdKafka::Event::EVENT_LOG:
        fprintf(stderr, "LOG-%i-%s: %s\n",
                event.severity(), event.fac().c_str(), event.str().c_str());
        break;

      case RdKafka::Event::EVENT_THROTTLE:
	std::cerr << "THROTTLED: " << event.throttle_time() << "ms by " <<
	  event.broker_name() << " id " << (int)event.broker_id() << std::endl;
	break;

      default:
        std::cerr << "EVENT " << event.type() <<
            " (" << RdKafka::err2str(event.err()) << "): " <<
            event.str() << std::endl;
        break;
    }
  }
private:
/**
 * @brief format a string timestamp from the current time
 */
static void print_time () {
#ifndef _WIN32
        struct timeval tv;
        char buf[64];
        gettimeofday(&tv, NULL);
        strftime(buf, sizeof(buf) - 1, "%Y-%m-%d %H:%M:%S", localtime(&tv.tv_sec));
        fprintf(stderr, "%s.%03d: ", buf, (int)(tv.tv_usec / 1000));
#else
        SYSTEMTIME lt = {0};
        GetLocalTime(&lt);
        // %Y-%m-%d %H:%M:%S.xxx:
        fprintf(stderr, "%04d-%02d-%02d %02d:%02d:%02d.%03d: ",
            lt.wYear, lt.wMonth, lt.wDay,
            lt.wHour, lt.wMinute, lt.wSecond, lt.wMilliseconds);
#endif
}

};

class DataConsumer {
public:
    using DataPtr_t = std::shared_ptr<RdKafka::Message>;

    DataConsumer() {
        this->m_conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
        this->m_tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

        std::string errstr;
        m_conf->set("rebalance_cb", &m_rebalance_cb, errstr);
        m_conf->set("enable.partition.eof", "true", errstr);
        m_conf->set("group.id", "group", errstr);
        m_conf->set("metadata.broker.list", "localhost", errstr);
        m_conf->set("default_topic_conf", m_tconf, errstr);
        m_conf->set("event_cb", &m_event_cb, errstr);

        m_consumer = RdKafka::KafkaConsumer::create(m_conf, errstr);
    }

    ~DataConsumer() {
        delete m_tconf;
        delete m_conf;
    }

    rxcpp::observable<DataPtr_t> getData() {
        return rxcpp::observable<>::create<DataPtr_t>([this](rxcpp::subscriber<DataPtr_t> s){
            RdKafka::ErrorCode err = m_consumer->subscribe({"test"});
            while (!err) {
                DataPtr_t msg(m_consumer->consume(1000));
                if(msg->err() == RdKafka::ERR_NO_ERROR)
                    s.on_next(msg);
            }
            s.on_completed();
        });
    }

private:
    RdKafka::Conf *m_conf;
    RdKafka::Conf *m_tconf;
    ExampleRebalanceCb m_rebalance_cb;
    ExampleEventCb m_event_cb;
    RdKafka::KafkaConsumer *m_consumer;
};

using namespace std;
int main() {
    //Sample 1 [[
    std::array<int, 3> a = {1, 2, 3};
    auto values = rxcpp::observable<>::iterate(a);
    values.subscribe([](int v){ cout << "Data: " << v << endl; },
                     [](){ cout << "Streaming is over" << endl; });
    //]] 1

    //Sample 2 [[
    struct Beer {
        string name;
        string country;
        double price;
    };  
    vector<Beer> beers {
            {"Stella", "Belgium", 9.50},
            {"Sam Adams", "USA", 8.50},
            {"Bud Light", "USA", 6.50},
            {"Brooklyn Lager", "USA", 8.00},
            {"  Sapporo", "Japan", 7.50}
    };
    rxcpp::observable<>::iterate(beers)
        .filter([](const Beer& beer){ return (beer.price < 8); })
        .map([](const Beer& b){ return b.name + ": $" + to_string(b.price); })
        .subscribe([](string beerStr) { cout << beerStr << endl;},
                   [](){ cout << "Streaming is over" << endl;});
    cout << "Last line of script" << endl;
    //]] 2

    // Sample 3 [[
    rxcpp::observable<>::iterate(beers)
        .map([](const Beer& beer){ return beer.price; })
        .reduce(0.0,
             [](double seed, double price) { return seed + price; },
             [](double total) { return total; }
             )
        .subscribe([](double tot) { cout << "Total :" << tot << endl;},
                   [](){ cout << "Streaming is over" << endl;}
            );
    cout << "Last line of script" << endl;
    //]] 3

    // Sample 4 [[
    auto ints = rxcpp::observable<>::create<int>([](rxcpp::subscriber<int> s){
        //Get data in async way       
        s.on_next(1);
        s.on_next(2);
        s.on_next(3);
        s.on_completed();
    });
    ints.subscribe([](int v){cout << "OnNext: "<< v << endl;},
                   [](){cout << "OnCompleted" << endl;});
    //]] 4

    //Sample 5 [[
    //----- create a subscription object 
    auto subscription = rxcpp::composite_subscription(); 
     
    //----- Create a Subscription  
    auto subscriber = rxcpp::make_subscriber<int>( 
        subscription, 
        [&](int data){
            cout << "Data : " << data << endl;
            if (data == 3)
                subscription.unsubscribe();
        }, 
        [](){ std::cout << "OnCompleted" << endl;}
    ); 
 
    rxcpp::observable<>::create<int>( 
        [](rxcpp::subscriber<int> s){ 
            for (int i = 0; i < 5; ++i) { 
                if (!s.is_subscribed())  
                    break; 
                s.on_next(i); 
           } 
            s.on_completed();   
    }).subscribe(subscriber);
    //]] 5

    //Sample 6 [[
    DataConsumer consumer;
    auto ob = consumer.getData()
        .subscribe_on(rxcpp::synchronize_new_thread())
        .map([](std::shared_ptr<RdKafka::Message> msg){
            string msgData = static_cast<const char *>(msg->payload());
            return msgData;})
        .window_with_time_or_count(std::chrono::seconds(30), 5, rxcpp::observe_on_event_loop());

    int window = 0;
    std::unordered_map<std::string, int> map;
    ob.subscribe([&window, &map](rxcpp::observable<std::string> msgs){
        int windowIndex = window++;
        msgs.subscribe([windowIndex, &map](const std::string& msg){
            cout << "[window " << windowIndex << "] OnNext: " << msg  << " count : "<< map.size() << endl;
            auto it = map.find(msg);
            if(it == map.end()) {
                cout << "insert " << msg << endl;
                map.insert({msg, 1});
            } else {
                cout << "increment " << msg << " " << it->second << endl;
                map[msg] = ++(it->second);
            }
        },
        [windowIndex, &map](){
            cout << "[window " << windowIndex << "]: Words count follows: " << map.size() << endl;
            for(auto &it : map) {
                cout << "\t" << it.first << ":" << it.second << endl;
            }    
            map.clear();
        });
    });

    char ch;
    std::cin >> ch;
    //]] 6

    return 0;
}