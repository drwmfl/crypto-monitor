import { useEffect, useState, useRef } from 'react';
import { Table, Card, Row, Col, Statistic, Tag, AutoComplete, Input, Empty } from 'antd';
import { FireOutlined, SearchOutlined, RiseOutlined } from '@ant-design/icons'; 
import axios from 'axios';

// ---------------- 辅助函数 ----------------

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

const getFilteredList = (list, text) => {
  if (!text) return list;
  const upperText = text.toUpperCase();
  return list.filter(item => {
    const cleanSymbol = item.symbol.replace('/USDT', '');
    return cleanSymbol.includes(upperText);
  });
};

// ---------------- 表格列定义 ----------------
const columns = [
  {
    title: '交易对',
    dataIndex: 'symbol',
    key: 'symbol',
    fixed: 'left',
    // 2. ✅ [优化] 宽度加宽到 180，防止长名字遮挡价格
    width: 150, 
    render: (text, record) => (
      <div style={{ display: 'flex', alignItems: 'center', fontWeight: 'bold' }}>
        {/* 点击跳转到币安 */}
        <a 
          // 3. ✅ [修复] 链接去掉了中间的斜杠 (比如 WLFI/USDT -> WLFIUSDT)
          href={`https://www.binance.com/zh-CN/futures/${text.replace('/', '')}`} 
          target="_blank" 
          rel="noopener noreferrer"
          style={{ color: '#1677ff', marginRight: 4 }}
        >
          {text}
        </a>
        
        {/* 1. ✅ [优化] 标签改为“现”，字体极小化 */}
        {record.has_spot && (
          <Tag color="cyan" style={{ 
            marginRight: 0, 
            fontSize: '10px',      // 字体改小
            lineHeight: '14px',    // 行高收紧
            padding: '0 3px',      // 内边距缩小
            transform: 'scale(0.9)', // 整体再稍微缩小一点点
            transformOrigin: 'left center' // 保证缩放不偏移
          }}>
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
    render: (val) => `$${parseFloat(val.toFixed(6))}`,
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
      return <span style={{ color }}>{val > 0 ? '+' : ''}{val.toFixed(2)}%</span>;
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

// ---------------- 主组件 ----------------
function App() {
  const searchTextRef = useRef('');
  const [data, setData] = useState([]);
  const [filteredData, setFilteredData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [searchText, setSearchText] = useState('');
  const [options, setOptions] = useState([]);


// 🔍 调试版 fetchData 函数 (请只粘贴这一段)
const fetchData = async () => {
  try {
    console.log("🚀 [1] 开始请求数据...")
    
    // 发起请求
    const res = await axios.get('/api/market-data')
    const newData = res.data

    console.log("📦 [2] 后端返回数据:", newData)
    console.log("📏 [3] 数据长度:", Array.isArray(newData) ? newData.length : "不是数组!")

    // 1. 设置原始数据
    setData(newData)
    //setLastUpdated(new Date())
    
    // 2. 获取当前搜索词
    // 这里加个保险，防止 searchTextRef 还没初始化
    const currentSearch = searchTextRef.current || ''
    console.log("🔍 [4] 当前搜索词:", currentSearch)

    // 3. 过滤并更新展示列表
    let list_to_show = []
    
    if (currentSearch) {
      // 如果有搜索词，执行过滤
      list_to_show = newData.filter(item => 
        item.symbol && item.symbol.toLowerCase().includes(currentSearch.toLowerCase())
      )
      console.log("✂️ [5] 过滤后剩余:", list_to_show.length)
    } else {
      // 如果没搜索词，显示全部
      list_to_show = newData
      console.log("✅ [5] 无搜索，显示全部:", list_to_show.length)
    }

    // 🔥 核心：这里是把数据放进表格的地方
    setFilteredData(list_to_show)
    
    // 4. 关闭加载状态
    setLoading(false)
    console.log("🏁 [6] 流程结束，Loading 已关闭")

  } catch (error) {
    console.error("❌ [CRITICAL] fetchData 发生崩溃:", error)
    setLoading(false)
  }
}

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 5000);
    return () => clearInterval(interval);
  }, []);

  // 监听搜索输入 (和数据变化)
  useEffect(() => {
    searchTextRef.current = searchText;
    
    const timeoutId = setTimeout(() => {
      // ✅ 这里的 data 现在是最新的了
      const results = filterDataList(data, searchText);
      setFilteredData(results);

      // 处理搜索建议
      if (searchText) {
        const suggestOptions = results.slice(0, 10).map(item => ({
          value: item.symbol.replace('USDT', ''), 
        }));
        setOptions(suggestOptions);
      } else {
        setOptions([]);
      }
    }, 300);

    return () => clearTimeout(timeoutId);
  }, [searchText, data]); // ✅✅✅ 关键修改：必须把 data 加进依赖数组！

  // ================= 统计计算逻辑 =================
  const squeezeCandidates = data.filter(i => i.funding_rate < 0).length;
  const highRisk = data.filter(i => i.oi_mc_ratio > 0.5).length;

  // 🔥 需求2：计算费率最负 (寻找最小值)
  let mostNegativeFundingItem = null;
  if (data.length > 0) {
    // 排序找最小
    const sortedByFunding = [...data].sort((a, b) => a.funding_rate - b.funding_rate);
    const minItem = sortedByFunding[0];
    // 必须小于 0 才算
    if (minItem && minItem.funding_rate < 0) {
      mostNegativeFundingItem = minItem;
    }
  }

  // ================= 搜索框渲染 =================
  const renderSearchBox = () => (
    <div style={{ width: '380px' }}> {/* 🔥 需求1：宽度加长 */}
      <AutoComplete
        options={options}
        style={{ width: '100%' }}
        onSelect={(val) => setSearchText(val)}
        onSearch={(val) => setSearchText(val)}
        value={searchText}
        placeholder="搜索代币 (如 BTC)"
        allowClear
      >
        <Input 
          size="large" // 大号输入框
          placeholder="搜索代币..." 
          prefix={<SearchOutlined style={{ color: '#1890ff', fontSize: '18px' }} />} // 图标也变大变色
          style={{ 
            // 🔥 需求1：样式增强 (高度、边框、阴影)
            height: '46px', 
            borderRadius: '8px',
            border: '2px solid #1890ff', // 醒目的蓝色边框
            boxShadow: '0 2px 6px rgba(24, 144, 255, 0.15)', // 增加阴影增加立体感
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
          最后更新: {new Date().toLocaleTimeString()}
        </span>
      </div>
      
      {/* 🔥 需求4：一行显示4个数据 */}
      <Row gutter={[16, 16]} style={{ marginBottom: 20 }}>
        
        {/* 🔥 需求2：费率最负 (第1位) */}
        <Col xs={24} sm={12} md={6}>
          <Card bordered={false} bodyStyle={{ padding: '12px 24px' }}>
            <Statistic 
              title="费率最负" 
              value={mostNegativeFundingItem ? (mostNegativeFundingItem.funding_rate * 100).toFixed(4) + '%' : '无'} 
              valueStyle={{ 
                color: '#cf1322', // 红色数值
                fontSize: '22px', 
                fontWeight: 'bold' 
              }} 
              // 前缀显示代币名称 (黑色)
              prefix={mostNegativeFundingItem ? <span style={{color: '#000', marginRight: '8px', fontSize: '18px'}}>{mostNegativeFundingItem.symbol.replace('/USDT','')}</span> : null}
            />
          </Card>
        </Col>

        {/* 🔥 需求3：负费率 (第2位，改名) */}
        <Col xs={24} sm={12} md={6}>
          <Card bordered={false} bodyStyle={{ padding: '12px 24px' }}>
            <Statistic 
              title="负费率 (逼空)" 
              value={squeezeCandidates} 
              valueStyle={{ color: '#cf1322' }} 
              suffix="个"
            />
          </Card>
        </Col>

        {/* 杠杆率 (第3位) */}
        <Col xs={24} sm={12} md={6}>
          <Card bordered={false} bodyStyle={{ padding: '12px 24px' }}>
            <Statistic 
              title="杠杆率 > 0.5 (高险)" 
              value={highRisk} 
              valueStyle={{ color: '#faad14' }} 
              suffix="个"
            />
          </Card>
        </Col>

        {/* 监控币种 (第4位) */}
        <Col xs={24} sm={12} md={6}>
          <Card bordered={false} bodyStyle={{ padding: '12px 24px' }}>
            <Statistic title="监控币种" value={data.length} suffix="个" />
          </Card>
        </Col>

      </Row>

      <Card 
        bordered={false} 
        bodyStyle={{ padding: 0 }}
        // 🔥 需求5：Title 样式更突出 (左侧竖线 + 大字号)
        title={
          <div style={{ 
            display: 'flex', 
            alignItems: 'center', 
            fontSize: '18px', 
            fontWeight: '800',
            color: '#1f1f1f'
          }}>
            <RiseOutlined style={{ marginRight: '8px', color: '#1890ff', fontSize: '22px' }} />
            <span style={{ position: 'relative', paddingLeft: '0px' }}>
              实时监控列表
            </span>
          </div>
        }
        extra={renderSearchBox()} 
      >
        <Table 
          columns={columns} 
          dataSource={filteredData} 
          rowKey="symbol"
          loading={loading}
          // 🔥 需求6：禁用表头悬停提示
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
                description={
                  <span style={{ color: '#8c8c8c' }}>
                    未搜索到结果 "{searchText}"
                  </span>
                } 
              />
            )
          }}
        />
      </Card>
    </div>
  );
}

export default App;