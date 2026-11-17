# Mô phỏng SFC trên nhiều data center

Chạy mã nguồn trực tiếp từ repository bằng `main.py --config <file.yaml>`. Mô-đun `virne.distributed` bổ sung bài toán DC phân cấp vào repository Virne; nó có vòng lặp sự kiện và solver riêng vì mô hình VNE cũ không phân biệt router, switch và server.

AHQ-SFC và bộ cấu hình hybrid được mô tả trong [hướng dẫn AHQ-SFC](ahq-sfc-running.md). Mọi cấu hình baseline và hybrid đều có controller delay và migration với cùng tham số hệ thống.

Các thí nghiệm chạy trên CPU; QAOA trong AHQ-SFC là mô phỏng statevector bằng NumPy.

## Cài thư viện và chạy

Python 3.12 trở lên. Trong thư mục repository, trên lệnh:

```bash
python -m pip install -r requirements-simulation.txt
python main.py --config configs/baselines/distributed_hre.yaml
```

Nếu Python đang dùng đã có NetworkX và PyYAML, chỉ cần:

```bash
python main.py --config configs/baselines/distributed_hre.yaml
python main.py --config configs/baselines/distributed_hre.yaml --validate-only
python main.py --help
```

Không cần cài package `virne`, PyTorch, PyG, Hydra hoặc OR-Tools cho các baseline DC này. Không thay đổi thư mục làm việc khi chạy. Đường dẫn YAML được tính từ thư mục gọi lệnh; `experiment.output_dir` cũng tính từ thư mục đó. Có thể dùng đường dẫn tuyệt đối, kể cả đường dẫn có dấu cách.

## Các baseline có sẵn

Mỗi file trong `configs/baselines/` là một cấu hình độc lập, có đủ tham số và không kế thừa file khác. Khi tự tạo YAML rút gọn, các khóa thiếu lấy mặc định từ `virne/distributed/config.py`; khóa không hợp lệ bị từ chối để phát hiện lỗi gõ.

| File YAML | Placement | Phạm vi |
| --- | --- | --- |
| `centralized_ffd.yaml` | First-Fit Decreasing | 1 DC, 64 server |
| `centralized_hre.yaml` | HRE-inspired | 1 DC, 64 server |
| `distributed_random.yaml` | Random với seed cố định | 4 DC × 16 server, mỗi SFC trong một DC |
| `distributed_ffd.yaml` | First-Fit Decreasing | 4 DC × 16 server, mỗi SFC trong một DC |
| `distributed_best_fit.yaml` | Best-Fit | 4 DC × 16 server, mỗi SFC trong một DC |
| `distributed_hre.yaml` | HRE-inspired | 4 DC × 16 server, mỗi SFC trong một DC |
| `distributed_cross_dc.yaml` | HRE-inspired, thêm tập server toàn mạng | Cho phép các VNF của một SFC ở nhiều DC |
| `distributed_hre_fat_tree.yaml` | HRE-inspired | 4 DC, mỗi DC là fat-tree k=4, 16 server |

Tất cả cấu hình trong `configs/baselines` có cùng 64 server, tổng 512 CPU core và 4096 GB RAM, cùng WAN, seed, workload, controller và tham số migration. Dùng `centralized_hre` với `distributed_hre` để so sánh vị trí DC; dùng bốn cấu hình distributed Random/FFD/Best-Fit/HRE để so sánh thuật toán. Cấu hình fat-tree thay đổi fabric nên được phân tích riêng ảnh hưởng của topology.

## Mô hình vật lý và workload

```text
WAN router / ingress
       |
   DC gateway
       |
  spine / core
       |
  leaf / aggregation + edge
       |
  physical servers (CPU, RAM)
```

- `topology.wan`: đồ thị router vô hướng với danh sách cạnh do người dùng chỉ định; mặc định là WAN tổng hợp gồm 4 điểm biên và 1 điểm trung tâm. Đây không phải topology Abilene của paper. `bandwidth_mbps: null` mô phỏng WAN không giới hạn băng thông; các đường nội bộ DC vẫn có giới hạn riêng.
- `topology.data_centers`: mỗi DC có ID, router WAN nối vào và số server. Có thể ghi đè `cpu` và `memory_gb` ngay trong từng DC để mô phỏng năng lực không đồng nhất. Gateway, switch và router không được dùng để đặt VNF.
- `topology.fabric.type: leaf_spine`: mỗi server nối vào một leaf; các leaf nối tới mọi spine; các spine nối gateway. Cho phép số server bất kỳ.
- `topology.fabric.type: fat_tree`: fat-tree đầy đủ k-ary, k chẵn, k pod, mỗi pod có k/2 edge và k/2 aggregation switch, tổng k²/4 core switch và k³/4 server **trong mỗi DC**. Gateway nối các core. Mọi DC dùng cùng k.
- Link access từ WAN router tới gateway dùng bandwidth và latency của fabric. Mọi link vô hướng có một dung lượng dùng chung cho cả hai chiều; không mô phỏng full-duplex thành hai dung lượng độc lập. WAN và fabric có độ trễ riêng.
- Arrival là Poisson, tức khoảng cách giữa các arrival có phân phối exponential; lifetime cũng exponential. Đơn vị cấu hình là request/giờ và giờ; event dùng giây. Yêu cầu được xử lý online, departure trước arrival nếu cùng thời điểm.
- Workload là **chuỗi tuyến tính có thứ tự**: ingress → VNF0 → VNF1 → ... với demand CPU/RAM cho từng VNF, bandwidth cho từng đoạn và SLO độ trễ toàn chuỗi. Có thể bật `return_to_ingress` để dự trữ cả đoạn cuối quay về ingress. Các demand lấy uniform trong khoảng `[min, max]`.
- `max_latency_ms: null` tắt SLO. Độ trễ bằng tổng độ trễ link trên các đoạn cộng processing delay cố định của từng VNF. Chưa có hàng đợi hay packet-level simulation; đây là mô hình admission/resource allocation.
- `solver.allow_colocation: false` yêu cầu các VNF **trong cùng một SFC** dùng server khác nhau. Các SFC khác nhau vẫn chia sẻ server nếu đủ tài nguyên. Colocation chỉ được hỗ trợ trong chế độ admission khi tắt migration; các cấu hình chuẩn giữ `false`.

Đổi quy mô bằng cách sửa `data_centers` và `num_servers` trong YAML. Muốn so sánh công bằng, giữ tổng CPU/RAM như nhau giữa các kiến trúc và giữ workload/seed.

## Mô tả các thuật toán

**Random:** xáo trộn thứ tự DC, server và thứ tự thử đặt VNF bằng RNG riêng, sau đó first-fit vào server đủ CPU/RAM. Không làm thay đổi RNG sinh workload.

**First-Fit Decreasing:** xếp VNF giảm dần theo block size `CPU × tổng bandwidth các đoạn kề VNF` (gồm đoạn ingress và return nếu có). Thử DC gần ingress theo độ trễ đường đi, rồi server theo thứ tự cố định.

**Best-Fit:** cùng thứ tự VNF và DC với FFD, nhưng chọn server có tổng CPU và RAM còn dư chuẩn hóa nhỏ nhất sau placement. Không có bước tối ưu năng lượng.

**HRE-inspired:** thử thứ tự FFD rồi n−1 thứ tự dạng pyramid: chuyển VNF có block size lớn nhất qua từng vị trí, phần bên trái tăng dần và bên phải giảm dần. Trong mỗi thứ tự, thử nhóm server cùng rack, cùng pod, toàn DC; leaf–spine không có pod riêng nên nhóm middle trùng toàn DC và được loại trùng. Server đang hoạt động được ưu tiên. Các placement ứng viên được xếp theo số server mới cần bật, giữ ưu tiên gần ingress khi bằng nhau. Với `cross_dc`, thêm một nhóm chứa tất cả server; nhóm này cũng tham gia xếp hạng năng lượng nên có thể được chọn dù vẫn còn một placement trong một DC.

Mọi baseline route các đoạn có bandwidth lớn trước trên đồ thị đã lọc theo dung lượng còn lại, bao gồm lượng dự trữ của các đoạn trước trong cùng yêu cầu. Random/FFD/Best-Fit dùng đường có độ trễ nhỏ nhất; HRE xét tối đa `k_paths` đường ngắn theo số hop rồi chọn ít công suất switch tăng thêm nhất khi bằng số hop. Server không được dùng làm node trung chuyển. Thứ tự **thử đặt** VNF không thay đổi thứ tự xử lý hoặc các cạnh của chuỗi gốc.

Placement và routing đều là tentative đến khi toàn bộ CPU, RAM, bandwidth và SLO hợp lệ. Khi thất bại, tài nguyên đang chạy không đổi; khi SFC hết hạn, mọi resource đã đặt được giải phóng. Các heuristic không tìm tất cả mapping/path: rejection chỉ có nghĩa thuật toán không tìm được nghiệm trong tập ứng viên.

## Migration trong vòng đời SFC

Migration bật mặc định trong `configs/baselines` và `configs/hybrid`. Cùng một heuristic classical định kỳ xét VNF trên server ít tải, thử chuyển sang server đang hoạt động và giữ phương án có lợi ích lớn nhất trong giới hạn candidate/budget. Mỗi lần chỉ chuyển một VNF, toàn hệ thống chỉ có một migration đang chạy. VNF đích luôn thuộc DC của VNF nguồn, kể cả khi SFC của baseline `cross_dc` trải qua nhiều DC.

Mô hình pre-copy giới hạn số vòng dựa trên [SLAMIG](https://arxiv.org/html/2101.09716v1). Profile `state_mb` và `dirty_rate_mbps` được sinh bằng RNG riêng, độc lập với RAM quota và workload. Nguồn tiếp tục phục vụ trong lúc copy; đích giữ trước CPU/RAM. Copy traffic và phần bandwidth tăng thêm của route đích cùng chiếm capacity, nên arrival mới phải tôn trọng các reservation này. Bandwidth copy giữ cố định trong mỗi migration.

Chỉ chuyển khi lợi ích vượt `minimum_improvement`:

```text
gain = energy_weight * (E_keep - E_move) / energy_reference_j
     + latency_weight * (D_old - D_new) / latency_reference_ms
     - migration_weight * C_migration

C_migration = transferred_Mbit * copy_path_hops / transfer_reference_mbit_hops
            + downtime_s / downtime_reference_s
            + route_changes / route_reference_changes
```

E_keep và E_move dùng cùng cửa sổ dự báo cố định H, không đọc lifetime còn lại hoặc arrival tương lai. E_move gồm thời gian nguồn và đích cùng giữ tài nguyên. Route changes đếm thêm/xóa cạnh có hướng theo từng đoạn chain. Request mới chưa có vị trí cũ nên migration cost bằng 0 ở admission; khi tái bố trí, cost được tính vào lựa chọn giữ hay chuyển và được ghi vào kết quả.

| Tham số trong `migration` | Mặc định |
| --- | --- |
| `interval_s`, `cooldown_s` | Xét mỗi 300 s; chờ 600 s sau mỗi lần chuyển của request |
| `max_candidates`, `budget_ms` | Tối đa 64 candidate và 250 ms CPU mỗi vòng |
| `profile.state_mb`, `profile.dirty_rate_mbps` | Uniform 64–256 MB và 1–10 Mbit/s |
| `bandwidth_mbps`, `max_precopy_rounds`, `timeout_s` | Cap 200 Mbit/s, tối đa 10 vòng và 60 s |
| `max_downtime_s`, `downtime_budget_s` | Tối đa 0,1 s/lần và 1 s/request |
| `copy_cpu`, `copy_buffer_mb` | Giữ thêm 0,1 core và 16 MB buffer ở mỗi đầu |
| `objective.migration_weight`, `objective.horizon_s` | Hệ số cost 0,1; cửa sổ dự báo 600 s |

Các khóa còn lại lấy mặc định từ `virne/distributed/config.py`. Profile và các mức tham chiếu là tham số mô phỏng tổng hợp, chưa hiệu chỉnh từ thiết bị thực. Giữ cùng toàn bộ cấu hình `migration` khi so sánh thuật toán; công cụ benchmark kiểm tra điều kiện này.

Controller dùng hai worker, admission deadline 1000 ms và latency `modeled` trong các preset. Admission được ưu tiên; quyết định migration dùng worker còn trống và trả worker khi bắt đầu copy. Candidate vượt capacity, timeout hoặc downtime SLA bị loại trước khi thực thi. Sau pre-copy, VNF dừng một thời gian ngắn để đồng bộ state còn lại, cutover thay allocation gốc và cleanup giải phóng tài nguyên cũ. Departure giữ nguyên timestamp từ admission, xử lý trước cutover cùng thời điểm; nếu request kết thúc trong migration thì hủy phần còn lại và ghi chi phí đã phát sinh.

## Công suất, năng lượng và kết quả

Server có VNF: `P = idle + (peak - idle) × CPU_utilization`; server rỗng dùng `server_off_w`. Khi migration, server đích được bật nhưng CPU giữ trước chưa được tính như một VNF đang xử lý traffic; CPU copy được tính riêng, và CPU dịch vụ của VNF nguồn được bỏ khỏi công suất động trong downtime. Switch có link đang sử dụng dùng `switch_idle_w + số port đang hoạt động × switch_port_w`; switch rỗng dùng `switch_off_w`. WAN router luôn bật nếu `wan_always_on: true`. Đếm port ở mỗi đầu switch của link; NIC của server nằm trong công suất server. Mô hình không có PUE/cooling, DVFS hoặc port speed thích nghi.

Tích phân công suất theo khoảng thời gian **giữa các sự kiện**, đổi W·s sang kWh bằng chia 3.600.000. Với controller bật, cửa sổ đo chung là `[0, max(arrival + admission_deadline + lifetime)]` của tất cả request được sinh, kể cả request bị từ chối. Migration không kéo dài lifetime hoặc cửa sổ đo. Utilization tính theo thời gian và bao gồm tài nguyên giữ trước; năng lượng tính theo công suất dịch vụ và overhead thực của các pha.

Mỗi lần chạy tạo thư mục riêng:

```text
results/distributed/<experiment.name>/<UTC-time>-<unique-id>/
  config.yaml       # cấu hình sau khi điền mặc định
  manifest.json     # file nguồn, checksum mã nguồn/workload, phiên bản Python/thư viện
  topology.json     # node/edge và tài nguyên ban đầu
  workload.json     # toàn bộ yêu cầu được sinh, kể cả yêu cầu bị từ chối
  allocations.json  # lịch sử placement/route với start_s, end_s, generation
  migration_profiles.json  # state và dirty rate theo request/VNF
  migration_decisions.json # candidate, lý do loại và thời gian quyết định
  migrations.json   # source/target, các pha, dữ liệu truyền, cost dự báo và thực tế
  migrations.csv    # số liệu phẳng, khi có migration bắt đầu
  outages.json      # các khoảng SFC ngừng phục vụ
  records.csv       # mỗi request: kết quả, latency, DC, thời gian solver
  events.csv        # mỗi sự kiện: active request/server/switch, utilization, power
  per_dc.csv        # số VNF/request phục vụ và utilization từng DC
  summary.json
  summary.csv
```

`summary` gồm acceptance ratio, rejection theo compute/bandwidth/latency, energy tổng/server/network, mean power, utilization, active server, latency trung bình/p95, cross-DC request và thời gian solver. Lý do rejection ghi ràng buộc sâu nhất đã thử: latency nếu từng route đủ bandwidth nhưng vi phạm SLO; bandwidth nếu từng placement đủ CPU/RAM nhưng route thất bại; còn lại compute.

`records.csv` có downtime và availability từng request. `mean_latency_ms`/`p95_latency_ms` giữ nghĩa tại admission; `time_weighted_service_latency_ms` tính theo lịch sử placement và loại downtime. Per-DC CPU/RAM tính theo thời gian từng placement, không nhân lại toàn lifetime sau mỗi lần chuyển.

`energy_kwh_per_accepted_request` là tổng năng lượng toàn hệ thống/các request được nhận, gồm cả năng lượng nền. Không gọi đại lượng này là công suất mỗi SFC. Khi không có request được nhận, latency/energy-per-request là null. Với WAN không giới hạn, utilization WAN là null vì không có mẫu số hữu hạn.

## Kiểm thử

```bash
python -m pip install -r requirements-hybrid.txt
python -m unittest discover -s tests/distributed -v
```

Bộ kiểm thử chung có thêm NumPy cho QAOA/policy offline. Các test kiểm tra topology, công bằng workload, CPU/RAM, chia sẻ băng thông, rollback, release, SLO, cross-DC, nguyên vẹn thứ tự chain, tích phân năng lượng, departure cùng thời điểm, seed và CLI từ ngoài repository. Bộ kiểm thử Virne cũ trong `tests/network`, `tests/core`, `tests/solver` vẫn cần dependency của framework cũ; `requirements-simulation.txt` chỉ phục vụ mô hình DC mới.
