// twin 是本地产线数字孪生的命令行入口：读取本地 JSON 配置，跑一次
// 确定性仿真，把产能、在制品与物料守恒结果打印到标准输出。
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/lechandonga/production-line-twin-go/internal/config"
	"github.com/lechandonga/production-line-twin-go/internal/sim"
)

func main() {
	path := flag.String("config", "configs/line.json", "本地产线配置文件路径(JSON)")
	traces := flag.Bool("traces", false, "打印每个制品的流转轨迹")
	flag.Parse()

	cfg, err := config.LoadFile(*path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "加载配置失败: %v\n", err)
		os.Exit(1)
	}
	s, err := sim.New(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "构建产线模型失败: %v\n", err)
		os.Exit(1)
	}
	r := s.Run()

	fmt.Printf("情景: %s (seed=%d)\n", r.Name, r.Seed)
	fmt.Printf("仿真结束时刻: %d ms\n", r.EndTimeMs)
	fmt.Printf("投入=%d 产出=%d 报废=%d 在制=%d (守恒校验: 投入 == 产出+报废+在制 -> %v)\n",
		r.Fed, r.Output, r.Scrap, r.WIP, r.Fed == r.Output+r.Scrap+int64(r.WIP))
	for i, st := range r.Stations {
		fmt.Printf("  工位 %-8s 完成=%-5d 质检失败=%-4d 故障=%-3d 次 维修停机=%d ms 末态缓冲=%d\n",
			st.ID, st.Processed, st.QualityFail, st.FaultCount, st.DownMs, r.BufferSizes[i])
	}
	if *traces {
		fmt.Println("制品轨迹:")
		for id := int64(1); id <= r.Fed; id++ {
			for _, t := range r.Traces[id] {
				fmt.Printf("  u%-4d @%-8d ms %-8s %-8s %s\n", t.UnitID, t.TimeMs, t.Station, t.Kind, t.Detail)
			}
		}
	}
}
