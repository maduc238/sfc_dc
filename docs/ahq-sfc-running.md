# Hướng dẫn chạy thuật toán Adaptive Hierarchical Hybrid SFC Embedding (AHQ-SFC)

AHQ-SFC đã được tích hợp dưới `solver.name: ahq`. Hệ thống mô phỏng hiện chạy trên CPU: decomposition, QUBO, exact enumeration, simulated annealing, QAOA statevector, policy chọn backend và controller có hàng đợi/deadline. QAOA dùng NumPy để mô phỏng thuật toán quantum trên CPU. Giữ nguyên giao diện `python main.py --config <file.yaml>` và không cần cài package `virne`.

## Chạy ngay

```bash
python -m pip install -r requirements-hybrid.txt
python main.py --config configs/hybrid/distributed_ahq_adaptive.yaml
python main.py --config configs/hybrid/distributed_hybrid_qaoa_sim.yaml
python -m unittest discover -s tests/distributed -v
```

`requirements-hybrid.txt` thêm NumPy để mô phỏng QAOA và fit policy offline. Exact/SA/rule policy chỉ cần `requirements-simulation.txt`. Không có QPU call khi chạy các preset được cung cấp. `--validate-only` kiểm tra config, topology và schema model nếu dùng policy đã học.

## Bộ cấu hình so sánh

Mỗi YAML trong `configs/hybrid` là một cấu hình độc lập. Tất cả dùng cùng controller, migration và hệ số cost với các baseline truyền thống; mô hình hệ thống được mô tả trong [tài liệu mô phỏng](distributed-data-centers.md).

| File | Cơ chế |
| --- | --- |
| `distributed_ahq_adaptive.yaml` | Rule policy: keep/exact/SA/quantum khi đủ điều kiện |
| `distributed_hybrid_exact.yaml` | Cùng decomposition, luôn chọn exact |
| `distributed_hybrid_sa.yaml` | Cùng decomposition, luôn chọn classical SA |
| `distributed_hybrid_qaoa_sim.yaml` | Cùng decomposition, luôn thử QAOA simulator |
| `distributed_hybrid_static_qa.yaml` | Adapter QA; mặc định disabled, kiểm tra fallback SA |
| `distributed_hybrid_adaptive_qa.yaml` | Rule policy hướng QA; mặc định chưa cho phép QPU |
| `distributed_hybrid_learned.yaml` | Model offline mẫu, fit trên seed 11/22, đánh giá seed 42 |
| `distributed_control_hre.yaml` | HRE không reduction, cùng controller/workload |
| `distributed_control_best_fit.yaml` | Best-Fit không reduction, cùng controller/workload |

Hai preset QA ở trạng thái mặc định **không phải benchmark phần cứng quantum**. Preset static QA disabled có thể khác SA vì một lần thử disabled chiếm slot call và fallback dùng stream seed riêng.

Để so sánh trong môi trường CPU, dùng HRE/Best-Fit, exact, SA, QAOA simulator và adaptive với `quantum_backend: qaoa_sim`. Giữ `hybrid.qa.enabled: false`. Hai preset QA disabled chỉ kiểm tra fallback, không cung cấp kết quả mô phỏng quantum annealing.

Chạy nhiều seed và xuất bảng tổng hợp:

```bash
python -m virne.distributed.hybrid.benchmark --configs configs/hybrid/distributed_control_hre.yaml configs/hybrid/distributed_hybrid_exact.yaml configs/hybrid/distributed_hybrid_sa.yaml configs/hybrid/distributed_hybrid_qaoa_sim.yaml configs/hybrid/distributed_ahq_adaptive.yaml --seeds 11 22 33 --output results/hybrid/my-comparison
```

Thư mục output phải chưa tồn tại. Công cụ kiểm tra workload/controller giống nhau và đối chiếu workload hash theo seed, lưu từng run cùng `comparison.csv/json` và `aggregate.csv/json`. Khoảng bootstrap 95% dựa trên các seed đã chạy; một seed không có khoảng tin cậy. Ba seed phù hợp kiểm tra workflow, chưa đủ để kết luận hiệu quả nghiên cứu. Không gộp kết quả có topology, workload hoặc budget khác nhau mà không phân tích riêng.

## Quyết định placement

1. Tạo incumbent bằng HRE (hoặc Best-Fit/FFD theo `incumbent_solver`).
2. Chọn tối đa `max_dcs` DC, ưu tiên DC của incumbent và proximity tới ingress.
3. Chọn tối đa `max_neighborhoods` tập VNF cho mỗi DC, kích thước `neighborhood_size`. Ưu tiên các VNF ít host khả thi, demand bandwidth lớn và VNF chưa được đặt. Cố định các VNF khác; khi cần, tạo seed placement chỉ xét compute.
4. Lọc CPU/RAM, cấm dùng host của VNF cố định, kiểm tra đường residual tới ingress/neighbor đã cố định khi có. Thu hẹp còn `candidates_per_vnf` host và giữ host của incumbent. Các kiểm tra đường theo cặp chưa bảo đảm bandwidth toàn chuỗi.
5. Sinh QUBO, chọn backend, decode rồi route lại toàn bộ chain bằng validator chung.
6. Chỉ thay incumbent khi có nghiệm khả thi tốt hơn theo objective thực. Khi không có incumbent, nghiệm khả thi tìm được có thể cứu request. Không mutate substrate trong quá trình search. Kết quả quantum lỗi/không có mẫu khả thi được thử SA trên cùng QUBO nếu còn ngân sách; luôn giữ incumbent làm fallback.

Admission AHQ-SFC chỉ hỗ trợ `scope: single_dc` và `allow_colocation: false`; cấu hình trái với giả định bị từ chối. `hybrid.optimize_existing` chỉ tối ưu incumbent chưa commit của request đang xử lý. Tái bố trí VNF của SFC đã nhận thuộc vòng vận hành chung, bật sẵn trong YAML. Candidate reduction và routing hữu hạn có thể loại mất nghiệm tốt hoặc khả thi.

## QUBO và objective thực

QUBO gồm chi phí server power tăng thêm + delay đường đi ước lượng, penalty one-hot và distinct-host. Penalty bằng tổng trị tuyệt đối các hệ số objective trước penalty cộng 1. CPU/RAM được bảo đảm bằng prefilter cộng distinct-host trong snapshot. Không đưa network power theo hợp switch hoặc BW/SLA toàn chuỗi vào QUBO như một mô hình chính xác. Validator kiểm tra lại đầy đủ các ràng buộc sau decode.

Xếp hạng nghiệm đã validate bằng:

```text
J = power_weight * incremental_server_and_network_power / power_reference_w
  + latency_weight * actual_chain_latency / latency_reference_ms
```

Incremental network power đếm mỗi switch/port mới một lần trên hợp đường đi. Đây là công suất (W), không gọi nhầm là năng lượng. Tổng năng lượng của thí nghiệm vẫn tích phân theo thời gian sự kiện. Objective trên dùng cho admission; request mới chưa có vị trí cũ nên migration cost bằng 0. Scheduler migration có objective riêng để so giữ nguyên với chuyển vị trí, gồm năng lượng dự báo, latency và cost chuyển.

`exact` duyệt assignment one-hot trong candidate pool, loại trùng host, rồi chấm qua routing policy. `neighborhood_search_complete` chỉ đúng nếu duyệt đủ và validate đủ. `neighborhood_optimal` còn yêu cầu tìm được nghiệm khả thi; chỉ tối ưu **trong neighborhood với routing policy cố định**, không phải optimum toàn mạng. Không có CP-SAT/MILP joint placement-routing trong phiên bản này.

## Backend chạy trên CPU

- **SA:** classical bit-flip Metropolis, seed riêng, lịch beta, bắt đầu một read từ seed placement và các read khác ngẫu nhiên. Mỗi read trả mẫu tốt nhất.
- **QAOA simulator:** NumPy thực thi statevector của cost diagonal QUBO và mixer X, bắt đầu ở trạng thái |+>. Optimizer classical là pattern search có seed, dùng expectation chính xác; chỉ bước đo cuối lấy `shots`. Có tùy chọn bit-flip ở readout, không phải noise model đầy đủ của thiết bị. `max_variables` mặc định 16, hard cap 20 để giới hạn RAM. Metadata ghi logical width, layers, evaluations, shots và số ZZ term; physical qubits/transpiled depth là null vì không có QPU hoặc transpiler ở đường chạy này.
SA và QAOA simulator dùng chung QUBO nhưng thực thi hai thuật toán khác nhau. Đo wall time CPU, chất lượng placement và số lần đánh giá; không coi thời gian statevector là thời gian thực thi circuit trên QPU.

## Adapter QPU ngoài phạm vi thí nghiệm CPU

Repository còn adapter `DWaveSampler` + `EmbeddingComposite`, lazy import SDK, worker process riêng để giới hạn chờ SDK/network. Khi có response, ghi timing do SDK trả, physical qubits, max chain length và chain-break fraction nếu có. Adapter này chưa được xác thực bằng lần chạy QPU thật và không được bật trong thí nghiệm CPU.

Để tự chạy QA thật, cài `requirements-quantum.txt`, cấu hình credential bằng cơ chế của D-Wave SDK (không ghi token vào YAML), bật `hybrid.qa.enabled`, đặt `max_calls_per_run` dương và budget chi phí phù hợp. Chọn static backend `qa` hoặc hiệu chỉnh rule policy để cho phép chọn QA. Cần tăng deadline/budget phù hợp với queue thực tế; 1000 ms trong preset là tham số demo, không cam kết QPU đáp ứng.

`estimated_cost_per_call` và `max_cost_per_run` là ước lượng do người dùng cung cấp, **không phải báo giá hay giới hạn thanh toán của nhà cung cấp**. Số lần gọi được chặn cứng. Khi timeout, process local bị dừng nhưng job đã gửi có thể vẫn chạy và bị tính phí; log `remote_completion_unknown` và vẫn tính dispatch attempt. Không suy diễn QPU usage bằng số logical variables hay thời gian simulated annealing.

Các RNG có seed riêng để không thay đổi offered workload. Tuy nhiên wall-time budget có thể cắt search ở bước khác nhau khi host bận, kể cả khi controller dùng modeled latency. Vì vậy cùng seed không bảo đảm placement giống hệt nếu budget chạm giới hạn. Log ghi budget exhaustion, QUBO và sample histogram; khi kiểm tra tái lập thuật toán nên dùng budget đủ rộng rồi đánh giá deadline trong thí nghiệm riêng. QPU vật lý cũng không được giả định là deterministic theo seed.

## Policy thích ứng và model offline

`policy.name: rule` ưu tiên exact nếu số assignment và thời gian dự báo vừa ngân sách; nếu không, chỉ chọn QAOA simulator khi backend khả dụng, expected gain vượt ngưỡng và expected time vừa ngân sách CPU. Thí nghiệm CPU giữ `quantum_backend: qaoa_sim`. Mặc định `quantum_expected_gain: 0` vì chưa có calibration. Các con số gain/time cấu hình là giả định, không phải kết quả đo tự động. Khi chưa có calibration cho QAOA, chọn SA. Đặt `optimize_existing: false` để nhận luôn incumbent khả thi của request mới.

`policy.name: learned` dùng mô hình action-value tuyến tính regularized, fit offline từ log rồi giữ cố định khi đánh giá. Đây là thành phần exploitation của contextual bandit; chưa có online exploration hoặc học chọn kích thước neighborhood. Feature gồm incumbent, số assignment, candidate overlap/retention, utilization, BW headroom và SLA slack. Không đọc future arrival hoặc lifetime tương lai.

Fit từ **training runs**, không dùng held-out test runs:

```bash
python -m virne.distributed.hybrid.policy --training-log results/train-exact/backend_calls.json results/train-sa/backend_calls.json results/train-qaoa/backend_calls.json --output results/models/ahq-policy.json
```

Các đường dẫn trên là ví dụ; thay bằng file thực trong thư mục run đã tạo. Sau đó sao chép YAML adaptive, đổi tên experiment và:

```yaml
hybrid:
  policy:
    name: learned
    model_path: ../../results/models/ahq-policy.json
```

`model_path` tương đối được tính từ thư mục YAML (ví dụ trên cho file trong `configs/hybrid`); `experiment.output_dir` vẫn tính từ thư mục gọi lệnh. Giữ các phần khác của YAML, thay seed/topology/load để đánh giá held-out. Model được snapshot vào output và ghi hash. `reward` là cải thiện objective hoặc +1 cho rescue, trừ wall time backend+validation chuẩn hóa theo decision budget; proposal bị stale/deadline có thêm penalty -1. `proposal_reward` giữ giá trị trước completion. Đây là surrogate learning reward, không trực tiếp bằng acceptance dài hạn hoặc chi phí tiền. Log quan sát chịu selection bias; ưu tiên thu thập cân bằng backend và làm ablation, không coi fit offline là chứng minh causal gain.

Repository kèm `configs/hybrid/models/example_policy.json` để chạy ngay preset learned. Model này được fit từ sáu run static exact/SA/QAOA ở seed 11 và 22, mỗi run 100 request. Provenance, training hash và nhãn `example_only` được lưu trong model. Đây là demo pipeline, chưa phải model được hiệu chỉnh cho workload thực hay kết quả chứng minh vượt baseline.

## Controller và thời gian

`control_plane.enabled: true` bật hàng đợi FCFS, số worker cố định và deadline tính từ arrival. Khi worker bắt đầu, solver tính proposal trên snapshot hiện tại; proposal được giao tại event `decision_complete`. Những request đang tính chưa giữ trước tài nguyên. Trước commit, audit toàn bộ proposal trên trạng thái mới; nếu không còn đủ tài nguyên thì thử incumbent đã tính sẵn, hoặc từ chối `stale`. Không chạy lại solver miễn phí ở completion.

Ba chế độ latency:

- `modeled`: base + validation + tổng latency cấu hình của các backend thực sự được thử. Backend disabled/unavailable/size-limit không chịu latency QPU giả. Đây là mô hình khai báo, không phải phép đo hardware.
- `measured`: tổng wall time solve (gồm preprocessing, formulation, sampling, decode và routing) + overhead audit cấu hình. Wall time còn phụ thuộc host.
- `trace`: `trace_ms` chứa một thời gian xử lý toàn quyết định cho mỗi request, theo request ID; dùng để replay, không cộng lại queue vào trace.

Queue delay được tính riêng và tính vào admission deadline. Departure được xử lý trước completion cùng timestamp; completion đúng deadline được chấp nhận. Lifetime bắt đầu lúc admission thành công. Cửa sổ tích phân chung của thí nghiệm controller là `max(offered_arrival + deadline + lifetime)`, kể cả request bị từ chối. Backend search có wall budget riêng và giới hạn call/validation/read/evaluation; checks là cooperative ở ranh giới phép toán, không bảo đảm hard real-time OS.

Mô phỏng concurrency là concurrency của quyết định trên snapshot/event time; không chạy nhiều solver CPU thực song song. Chế độ `measured` không mô hình hóa CPU contention giữa các worker. Timeout trong simulator không tự hủy job QPU trên provider. Đánh giá hệ thống thực cần đo queue, controller và backend riêng.

## Kết quả thu được

Ngoài các file cũ, mỗi làn chạy có:

- `backend_calls.json`: action, loại backend, status, seed, features, QUBO đầy đủ, histogram bitstring, raw placement feasibility, routing feasibility, objective thực, chi phí/thời gian, optimality scope và reward.
- `decisions.json`: incumbent, rescue/improvement, budget exhausted, policy reasons, snapshot version và kết quả admission cuối cùng.
- `records.csv`: queue/service/admission delay, snapshot/commit version, fallback và lý do từ chối.
- `policy_model.json` khi dùng model offline, kèm hash trong manifest.

`summary` thêm deadline/stale/budget rejection, admission percentiles, số call classical/QAOA/QPU, estimated QPU cost, sample feasibility và admitted rescues. Phân biệt `rescued_proposals` với `admitted_rescues`: proposal tốt vẫn có thể bị quá deadline hoặc không còn tài nguyên tại completion. Không bỏ các lần gọi lỗi, sample không khả thi hoặc timeout khỏi đánh giá.

## Trạng thái kiểm chứng và phạm vi còn lại

Kiểm thử local bao gồm QUBO exhaustive, statevector QAOA đối chiếu nghiệm giải tích, seed, rescue, incumbent preservation, fallback, CPU/RAM/BW/SLA, queue, deadline, stale snapshot, tích phân năng lượng và hợp đồng SDK D-Wave bằng test double. Chưa xác thực trên QPU thật và chưa chứng minh quantum speedup.

HRE và AHQ-SFC dùng cùng scheduler migration classical, cùng overhead và cost. QAOA chưa giải reconfiguration. Chi tiết state transfer, reservation, downtime và kết quả nằm trong [mô hình hệ thống](distributed-data-centers.md).

AHQ-SFC cross-DC chain, CP-SAT joint optimum, online bandit exploration và model noise thiết bị đầy đủ chưa được triển khai. Các baseline cross-DC classical cũ vẫn dùng được riêng. Xem [note thiết kế AHQ-SFC](adaptive-hybrid-quantum-sfc.md) để biết cơ sở lựa chọn.
