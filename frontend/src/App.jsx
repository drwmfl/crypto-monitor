import { useEffect, useState, useMemo } from 'react';
import { Table, Card, Row, Col, Statistic, Tag, AutoComplete, Input, Empty } from 'antd';
import { FireOutlined, SearchOutlined, RiseOutlined } from '@ant-design/icons'; 
import axios from 'axios';

// ---------------- 1. 静态辅助函数 ----------------

const formatLargeNumber = (val) => {
  if (!val || val === 0) return '-';
  if (val >= 1000000000) return `$${(val / 1000000000).toFixed(2)}B`;
  return `$${(val / 1000000).toFixed(2)}M`;
};

const formatFullCurrency = (val) => {
  if (val === undefined || val === null) return '$0.00';
  return val.toLocaleString('en-US', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  });
};

// ---------------- 2. 表格列定义 ----------------
const columns = [
  {
    title: '交易对',
    dataIndex: 'symbol',
    key: 'symbol',
    fixed: 'left',
    width: 150, 
    render: (text, record) => (
      <div style={{ display: 'flex', alignItems: 'center', fontWeight: 'bold' }}>
        <a 
          href={`https://www.binance.com/zh-CN/futures/${text.replace('/', '')}`} 
          target="_blank" 
          rel="noopener noreferrer"
          style={{ color: '#1677ff', marginRight: 4 }}
        >
          {text}
        </a>
        {record.has_spot && (
          <Tag color="cyan" style={{ marginRight: 0, fontSize: '10px', lineHeight: '14px', padding: '0 3px', transform: 'scale(0.9)', transformOrigin: 'left center' }}>
            现
          </Tag>
        )}
      </div>
    ),
  },
  {
    title: '价格',
    dataIndex: 'price',
    key: 'price',
    width: 100,
    render: (val) => {
        // 增加颜色跳动提示的逻辑可以放在这里，目前先保持基础展示
        return <span style={{ fontWeight: '600' }}>${parseFloat(val).toFixed(5)}</span>;
    },
  },
  {
    title: '1h 涨跌',
    dataIndex: 'change_1h',
    key: 'change_1h',
    width: 100,
    sorter: (a, b) => a.change_1h - b.change_1h,
    render: (val) => {
      const v = val || 0;
      const color = v > 0 ? '#388e3c' : v < 0 ? '#d32f2f' : 'black';
      return <span style={{ color }}>{v > 0 ? '+' : ''}{v.toFixed(2)}%</span>;
    },
  },
  {
    title: '24h 涨跌',
    dataIndex: 'change_24h',
    key: 'change_24h',
    width: 100,
    sorter: (a, b) => a.change_24h - b.change_24h,
    render: (val) => {
      const color = val > 0 ? '#388e3c' : val < 0 ? '#d32f2f' : 'black';
      return <span style={{ color }}>{val > 0 ? '+' : ''}{parseFloat(val).toFixed(2)}%</span>;
    },
  },
  {
    title: '流通市值(MC)',
    dataIndex: 'mc',
    key: 'mc',
    width: 130,
    sorter: (a, b) => a.mc - b.mc,
    render: (val) => formatLargeNumber(val),
  },
  {
    title: '全流通(FDV)',
    dataIndex: 'fdv',
    key: 'fdv',
    width: 130,
    sorter: (a, b) => a.fdv - b.fdv,
    render: (val) => formatLargeNumber(val),
  },
  {
    title: '费率',
    dataIndex: 'funding_rate',
    key: 'funding_rate',
    width: 110,
    sorter: (a, b) => a.funding_rate - b.funding_rate,
    render: (val) => {
      const pct = val * 100;
      let color = 'black'; 
      if (pct < 0) color = '#d32f2f'; 
      else if (pct > 0.005) color = '#388e3c'; 
      return <span style={{ color }}>{pct.toFixed(5)}%</span>;
    },
  },
  {
    title: '费率周期',
    dataIndex: 'listing_hours',
    key: 'listing_hours',
    width: 100,
    render: (val) => {
      if (val === 1) return <Tag color="#f50"><FireOutlined /> 1h</Tag>;
      return <Tag>{val}h</Tag>;
    },
  },
  {
    title: '24h成交量',
    dataIndex: 'volume_24h',
    key: 'volume_24h',
    width: 130,
    sorter: (a, b) => a.volume_24h - b.volume_24h,
    render: (val) => formatLargeNumber(val),
  },
  {
    title: '持仓量 (OI)',
    dataIndex: 'oi',
    key: 'oi',
    width: 130,
    sorter: (a, b) => a.oi - b.oi,
    render: (val) => formatLargeNumber(val),
  },
  {
    title: 'OI变动',
    dataIndex: 'oi_change_val',
    key: 'oi_change_val',
    width: 140,
    sorter: (a, b) => a.oi_change_val - b.oi_change_val,
    render: (val) => {
        const v = val || 0;
        const color = v > 0 ? '#388e3c' : v < 0 ? '#d32f2f' : 'black';
        const absVal = Math.abs(v);
        const formatted = formatFullCurrency(absVal);
        const sign = v > 0 ? '' : v < 0 ? '-' : '';
        return <span style={{ color }}>{sign}${formatted}</span>;
    },
  },
  {
    title: 'OI涨跌',
    dataIndex: 'oi_change_pct',
    key: 'oi_change_pct',
    width: 110,
    sorter: (a, b) => a.oi_change_pct - b.oi_change_pct,
    render: (val) => {
      const v = (val || 0) * 100;
      const color = v > 0 ? '#388e3c' : v < 0 ? '#d32f2f' : 'black';
      return <span style={{ color }}>{v > 0 ? '' : ''}{v.toFixed(2)}%</span>;
    },
  },
  {
    title: '杠杆率',
    dataIndex: 'oi_mc_ratio',
    key: 'oi_mc_ratio',
    width: 100,
    sorter: (a, b) => a.oi_mc_ratio - b.oi_mc_ratio,
    render: (val) => {
      const style = val > 0.5 ? { color: 'red', fontWeight: 'bold' } : {};
      return <span style={style}>{val.toFixed(3)}</span>;
    },
  },
  {
    title: 'OI/VOL',
    dataIndex: 'oi_vol_ratio',
    key: 'oi_vol_ratio',
    width: 100,
    sorter: (a, b) => a.oi_vol_ratio - b.oi_vol_ratio,
    render: (val) => val.toFixed(2),
  }
];

// ---------------- 3. 主组件 ----------------
function App() {
  const [data, setData] = useState([]); // 原始全量数据
  const [loading, setLoading] = useState(true);

  // 1. inputValue: 绑定输入框，实时响应
  const [inputValue, setInputValue] = useState(''); 
  
  // 2. debouncedSearchText: 延迟更新的搜索词
  const [debouncedSearchText, setDebouncedSearchText] = useState('');

  // 🔄 慢车道：获取全量数据 (OI, 市值等) - 5分钟一次
  const fetchFullData = async () => {
    try {
      const res = await axios.get('/api/market-data');
      setData(res.data);
      setLoading(false);
    } catch (error) {
      console.error("Fetch Full Data Error:", error);
      setLoading(false);
    }
  };

  // ⚡️ 快车道：获取实时价格 (Redis) - 2秒一次
  const fetchRealtimePrice = async () => {
    // 只有当表格里有数据时才去刷价格
    if (data.length === 0) return;

    try {
      const res = await axios.get('/api/market-data/realtime');
      const realtimeList = res.data || [];

      // 将数组转为 Map，方便 O(1) 查找
      const realtimeMap = new Map();
      realtimeList.forEach(item => {
        realtimeMap.set(item.symbol, item);
      });

      // 合并数据
      setData(prevData => {
        return prevData.map(item => {
          // 后端 Redis 返回的是 "BTCUSDT"，前端表格里是 "BTC/USDT"
          // 需要去掉斜杠来匹配
          const lookupKey = item.symbol.replace('/', '');
          const realtime = realtimeMap.get(lookupKey);

          if (realtime) {
            // 只更新变动的字段
            return {
              ...item,
              price: realtime.price,
              change_24h: realtime.change_24h,
              volume_24h: realtime.volume_24h,

              // ✅ [新增] 如果 Redis 里有市值，也一并更新！
              // 注意：如果后端算出是 0 (没取到supply)，就保留原来的 item.mc
              mc: realtime.mc > 0 ? realtime.mc : item.mc,
              fdv: realtime.fdv > 0 ? realtime.fdv : item.fdv,
              // 🔥 [新增] 实时更新费率
              // 只要 realtime 里有费率 (不为 undefined)，就用实时的，否则用数据库旧值
              funding_rate: (realtime.funding_rate !== undefined) ? realtime.funding_rate : item.funding_rate
            };
          }
          return item;
        });
      });
    } catch (error) {
      console.error("Realtime Fetch Error:", error);
    }
  };

  // 初始化：启动慢轮询 (5分钟)
  useEffect(() => {
    fetchFullData();
    const interval = setInterval(fetchFullData, 5 * 60 * 1000); 
    return () => clearInterval(interval);
  }, []);

  // 启动快轮询 (2秒) - 依赖 data.length 确保初始化后再开始
  useEffect(() => {
    if (data.length === 0) return;

    const interval = setInterval(fetchRealtimePrice, 2000);
    return () => clearInterval(interval);
  }, [data.length]); 
  // 注意：这里依赖 data.length 只是为了启动定时器。
  // 定时器内部使用的是 setData(prev => ...) 函数式更新，所以闭包里即使 data 是旧的也没关系。

  // ⚡️ 防抖搜索逻辑
  useEffect(() => {
    const handler = setTimeout(() => {
      setDebouncedSearchText(inputValue);
    }, 300);

    return () => clearTimeout(handler);
  }, [inputValue]);

  // 🔍 表格过滤逻辑
  const filteredData = useMemo(() => {
    if (!debouncedSearchText) return data;
    
    const upperText = debouncedSearchText.toUpperCase();
    
    return data.filter(item => {
      const symbol = item.symbol || '';
      const cleanSymbol = symbol.replace('/USDT', '');
      return symbol.includes(upperText) || cleanSymbol.includes(upperText);
    });
  }, [data, debouncedSearchText]); 

  // 💡 搜索建议逻辑
  const options = useMemo(() => {
    if (!inputValue) return [];
    
    const upperText = inputValue.toUpperCase();
    return data
      .filter(item => {
        const s = item.symbol || '';
        return s.toUpperCase().replace('/USDT', '').includes(upperText);
      })
      .slice(0, 10)
      .map(item => ({
        value: item.symbol.replace('/USDT', ''), 
      }));
  }, [data, inputValue]);

  // 处理输入变化
  const handleSearchChange = (value) => {
    setInputValue(value); 
  };

  // 处理选中建议
  const handleSelect = (value) => {
    setInputValue(value);
    setDebouncedSearchText(value);
  };

  // ================= 统计计算逻辑 =================
  const squeezeCandidates = data.filter(i => i.funding_rate < 0).length;
  const highRisk = data.filter(i => i.oi_mc_ratio > 0.5).length;

  let mostNegativeFundingItem = null;
  if (data.length > 0) {
    const sortedByFunding = [...data].sort((a, b) => a.funding_rate - b.funding_rate);
    const minItem = sortedByFunding[0];
    if (minItem && minItem.funding_rate < 0) {
      mostNegativeFundingItem = minItem;
    }
  }

  // ================= 搜索框渲染 =================
  const renderSearchBox = () => (
    <div style={{ width: '380px' }}> 
      <AutoComplete
        options={options}
        style={{ width: '100%' }}
        onSelect={handleSelect}
        onSearch={handleSearchChange}
        value={inputValue}
        placeholder="搜索代币 (如 BTC)"
        allowClear
      >
        <Input 
          size="large" 
          placeholder="搜索代币..." 
          prefix={<SearchOutlined style={{ color: '#1890ff', fontSize: '18px' }} />} 
          style={{ 
            height: '46px', 
            borderRadius: '8px',
            border: '2px solid #1890ff', 
            boxShadow: '0 2px 6px rgba(24, 144, 255, 0.15)', 
            fontSize: '16px'
          }}
        />
      </AutoComplete>
    </div>
  );

  return (
    <div style={{ padding: '24px', minHeight: '100vh', width: '100%', boxSizing: 'border-box' }}>
      
      <div style={{ marginBottom: '24px' }}>
        <h1 style={{ margin: 0, fontSize: '24px', fontWeight: 'bold', color: '#1f1f1f' }}>
          🦅 币安合约监控
        </h1>
        <span style={{ color: '#8c8c8c', fontSize: '12px' }}>
          系统状态: {loading ? '初始化数据中...' : '运行中'} | 实时更新: 已启用
        </span>
      </div>
      
      <Row gutter={[16, 16]} style={{ marginBottom: 20 }}>
        <Col xs={24} sm={12} md={6}>
          <Card bordered={false} bodyStyle={{ padding: '12px 24px' }}>
            <Statistic 
              title="费率最负" 
              value={mostNegativeFundingItem ? (mostNegativeFundingItem.funding_rate * 100).toFixed(4) + '%' : '无'} 
              valueStyle={{ color: '#cf1322', fontSize: '22px', fontWeight: 'bold' }} 
              prefix={mostNegativeFundingItem ? <span style={{color: '#000', marginRight: '8px', fontSize: '18px'}}>{mostNegativeFundingItem.symbol.replace('/USDT','')}</span> : null}
            />
          </Card>
        </Col>

        <Col xs={24} sm={12} md={6}>
          <Card bordered={false} bodyStyle={{ padding: '12px 24px' }}>
            <Statistic title="负费率 (逼空)" value={squeezeCandidates} valueStyle={{ color: '#cf1322' }} suffix="个" />
          </Card>
        </Col>

        <Col xs={24} sm={12} md={6}>
          <Card bordered={false} bodyStyle={{ padding: '12px 24px' }}>
            <Statistic title="杠杆率 > 0.5 (高险)" value={highRisk} valueStyle={{ color: '#faad14' }} suffix="个" />
          </Card>
        </Col>

        <Col xs={24} sm={12} md={6}>
          <Card bordered={false} bodyStyle={{ padding: '12px 24px' }}>
            <Statistic title="监控币种" value={data.length} suffix="个" />
          </Card>
        </Col>
      </Row>

      <Card 
        bordered={false} 
        bodyStyle={{ padding: 0 }}
        title={
          <div style={{ display: 'flex', alignItems: 'center', fontSize: '18px', fontWeight: '800', color: '#1f1f1f' }}>
            <RiseOutlined style={{ marginRight: '8px', color: '#1890ff', fontSize: '22px' }} />
            <span style={{ position: 'relative', paddingLeft: '0px' }}>实时监控列表</span>
          </div>
        }
        extra={renderSearchBox()} 
      >
        <Table 
          columns={columns} 
          dataSource={filteredData} 
          rowKey="symbol"
          loading={loading && data.length === 0}
          showSorterTooltip={false}
          pagination={{ 
            defaultPageSize: 20, showSizeChanger: true, 
            pageSizeOptions: ['20', '50', '100'], position: ['bottomCenter']
          }}
          size="middle"
          scroll={{ x: 1500 }}
          locale={{
            emptyText: (
              <Empty 
                image={Empty.PRESENTED_IMAGE_SIMPLE} 
                description={<span style={{ color: '#8c8c8c' }}>未搜索到结果 "{inputValue}"</span>} 
              />
            )
          }}
        />
      </Card>
    </div>
  );
}

export default App;