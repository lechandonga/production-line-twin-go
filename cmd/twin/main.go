// Command twin 在本地运行产线数字孪生仿真：读取 JSON 配置，按时间推进，
// 打印产能、在制品、工位与物料轨迹统计。完全离线，不访问网络或外部服务。
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/lechandonga/production-line-twin-go/internal/twin"
)

func main() {
	cfgPath := flag.String("config", "examples/line.json", "产线配置文件路径")
	seeds := flag.Int("seeds", 1, "并发运行的随机情景数（seed 取配置 seed 起的连续值）")
	traces := flag.Int("traces", 0, "打印前 N 件制品的流转轨迹（0 不打印）")
	flag.Parse()

	cfg, err := twin.LoadConfig(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}

	type slot struct {
		seed int64
		res  *twin.Result
		err  error
	}
	slots := make([]slot, *seeds)
	for i := range slots {
		slots[i].seed = cfg.Seed + int64(i)
	}

	for i := range slots {
		c := *cfg // 值拷贝配置，情景间互不干扰
		c.Seed = slots[i].seed
		sim, err := twin.New(&c)
		if err != nil {
			fmt.Fprintf(os.Stderr, "build model (seed=%d): %v\n", slots[i].seed, err)
			os.Exit(1)
		}
		slots[i].res, slots[i].err = sim.Run()
	}

	for _, sl := range slots {
		if sl.err != nil {
			fmt.Fprintf(os.Stderr, "simulation (seed=%d): %v\n", sl.seed, sl.err)
			os.Exit(1)
		}
		r := sl.res
		fmt.Printf("== scenario seed=%d ==\n", r.Seed)
		fmt.Printf("planned=%d released=%d completed=%d scrapped=%d wip=%d pending=%d avgWIP=%.3f lastEvent=%.3f\n",
			r.Planned, r.Released, r.Completed, r.Scrapped, r.WIP, r.Pending, r.AvgWIP, r.LastEventTime)
		for why, n := range r.ScrapReasons {
			fmt.Printf("  scrap[%s]=%d\n", why, n)
		}
		fmt.Print("  buffers:")
		for _, b := range r.BufferSnapshot {
			fmt.Printf(" %s=%d", b.Buffer, b.Items)
		}
		fmt.Println()
		for _, st := range r.Stations {
			fmt.Printf("  station %-8s processed=%-4d busy=%8.2f down=%8.2f util=%.3f\n",
				st.ID, st.Processed, st.BusyTime, st.Downtime, st.Utilization)
		}
		if *traces > 0 {
			n := *traces
			if n > len(r.Traces) {
				n = len(r.Traces)
			}
			for _, t := range r.Traces[:n] {
				fmt.Printf("  part %d: ", t.PartID)
				for _, e := range t.Events {
					fmt.Printf("[%.2f %s", e.Time, e.Kind)
					if e.Station != "" {
						fmt.Printf("@%s", e.Station)
					}
					if e.Buffer != "" {
						fmt.Printf("->%s", e.Buffer)
					}
					if e.Detail != "" {
						fmt.Printf("(%s)", e.Detail)
					}
					fmt.Print("] ")
				}
				fmt.Println()
			}
		}
	}
}
