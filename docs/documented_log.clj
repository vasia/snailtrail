;; Documented first epoch log of a simple dataflow computation involving w0 and w1
;; Perspective: w1
;; (Setup messages already parsed)

;; Findings
;; - messages track on a tuple level
;; - GMsgs allow seeing actual work (but don't work when notify isn't implemented)
;; - Msgs are sent when operator does some work (except for sinks)
;; - Msg scopes allow differentiating between work on w0-data and w1-data
;; - Separation between data arrived at an operator & progress unblocking that data
;; - data stored in arrangement instead of buffered at reduce

Dataflow: (0, Dataflow)
Dataflow: Channel 3: (1, Input) -> (2, Map)
Dataflow: Channel 5: (2, Map) -> (4, Arrange)
Dataflow: Channel 7: (4, Arrange) -> (6, Reduce)
Dataflow: Channel 9: (6, Reduce) -> (8, AsCollection)
Dataflow: Channel 11: (8, AsCollection) -> (10, InspectBatch)
Dataflow: Channel 13: (10, InspectBatch) -> (12, Probe)
Dataflow: (14, Progress Communication Channel)

(6.211704ms, 1, Text("[st] begin computation at epoch: 0"))                                      ;; start epoch 0

(6.275699ms, 1, Msg { is_send: true, channel: 3, source: 1, target: 1, seq_no: 0, length: 1 })   ;; start send data in->map
(6.485057ms, 1, Sched { id: 0, start_stop: Start })                                              ;; start dataflow
(6.5889ms, 1, Sched { id: 1, start_stop: Start })                                                ;; start in
(6.594618ms, 1, Sched { id: 1, start_stop: Stop })                                               ;; stop in 
(6.649996ms, 1, Sched { id: 2, start_stop: Start })                                              ;; start map
(6.659156ms, 1, Msg { is_send: false, channel: 3, source: 1, target: 1, seq_no: 0, length: 1 })  ;; stop send data in->map
(6.669499ms, 1, GMsg { is_start: true })                                                         ;; work @map
(6.693677ms, 1, GMsg { is_start: false })
(6.860111ms, 1, Msg { is_send: true, channel: 5, source: 1, target: 1, seq_no: 0, length: 1 })   ;; start send data map->arr
(6.883828ms, 1, Sched { id: 2, start_stop: Stop })                                               ;; stop map
(6.899824ms, 1, Sched { id: 4, start_stop: Start })                                              ;; start arr
(6.91746ms, 1, Msg { is_send: false, channel: 5, source: 0, target: 1, seq_no: 0, length: 2 })   ;; **recv arr data from w0!**
(6.921997ms, 1, GMsg { is_start: true })                                                         ;; work @arr (w0 data)
(6.95933ms, 1, GMsg { is_start: false })
(6.963154ms, 1, Msg { is_send: false, channel: 5, source: 1, target: 1, seq_no: 0, length: 1 })  ;; stop send data map->arr
(6.966712ms, 1, GMsg { is_start: true })                                                         ;; work @arr (map->arr data)
(7.000319ms, 1, GMsg { is_start: false })
(7.07546ms, 1, Sched { id: 4, start_stop: Stop })                                                ;; stop arr
(7.087426ms, 1, Sched { id: 6, start_stop: Start })                                              ;; start red...
(7.232571ms, 1, Sched { id: 6, start_stop: Stop })                                               ;; ...can't work (no w0 prog comm)
(7.245391ms, 1, Sched { id: 8, start_stop: Start })                                              ;; start as_coll... 
(7.347722ms, 1, Sched { id: 8, start_stop: Stop })                                               ;; ...can't work (waits for red)
(7.358676ms, 1, Sched { id: 10, start_stop: Start })                                             ;; start inspect...
(7.451233ms, 1, Sched { id: 10, start_stop: Stop })                                              ;; ...can't work (waits for as_coll)
(7.47434ms, 1, Sched { id: 12, start_stop: Start })                                              ;; start probe...
(7.485335ms, 1, Sched { id: 12, start_stop: Stop })                                              ;; ...can't work (waits for inspect)
(7.504972ms, 1, Progress { is_send: true, source: 1, channel: 14, seq_no: 0, addr: [0]})         ;; **start own progress comm!**
(7.520391ms, 1, Sched { id: 0, start_stop: Stop })                                               ;; stop dataflow

(7.683499ms, 1, Sched { id: 0, start_stop: Start })                                              ;; start dataflow 
(7.689996ms, 1, Progress { is_send: false, source: 0, channel: 14, seq_no: 0, addr: [0]})        ;; **recv progress from w0!**
(7.696624ms, 1, Progress { is_send: false, source: 1, channel: 14, seq_no: 0, addr: [0]})        ;; **stop own progress comm!** 
(7.870198ms, 1, PushProgress { op_id: 2 })                                                       ;; epoch has progressed to map
(7.870997ms, 1, Sched { id: 2, start_stop: Start })                                              ;; wrap up map
(7.953426ms, 1, Sched { id: 2, start_stop: Stop })                                               ;; (nothing to do)
(7.961319ms, 1, PushProgress { op_id: 4 })                                                       ;; epoch has progressed to arr
(7.961785ms, 1, Sched { id: 4, start_stop: Start })                                              ;; wrap up arr
(8.103914ms, 1, Msg { is_send: true, channel: 7, source: 1, target: 1, seq_no: 0, length: 1 })   ;; **new data for arr->red!**
(8.116241ms, 1, Sched { id: 4, start_stop: Stop })                                               ;; stop arr
(8.124906ms, 1, Sched { id: 6, start_stop: Start })                                              ;; start red
(8.131063ms, 1, Msg { is_send: false, channel: 7, source: 1, target: 1, seq_no: 0, length: 1 })  ;; stop send data arr->red
(8.133574ms, 1, GMsg { is_start: true })                                                         ;; work @red (combined data)
(8.155502ms, 1, GMsg { is_start: false })                                                        
(8.531332ms, 1, Msg { is_send: true, channel: 9, source: 1, target: 1, seq_no: 0, length: 1 })   ;; start send data red->as_coll
(8.54381ms, 1, Sched { id: 6, start_stop: Stop })                                                ;; stop red
(8.551633ms, 1, Sched { id: 8, start_stop: Start })                                              ;; start as_coll
(8.55614ms, 1, Msg { is_send: false, channel: 9, source: 1, target: 1, seq_no: 0, length: 1 })   ;; stop send data red->as_coll
(8.55878ms, 1, GMsg { is_start: true })                                                          ;; work @as_coll (combined data) 
(8.569486ms, 1, GMsg { is_start: false })
(8.638371ms, 1, Msg { is_send: true, channel: 11, source: 1, target: 1, seq_no: 0, length: 1 })  ;; start send data as_coll->inspect
(8.64966ms, 1, Sched { id: 8, start_stop: Stop })                                                ;; stop as_coll
(8.656632ms, 1, Sched { id: 10, start_stop: Start })                                             ;; start inspect
(8.660747ms, 1, Msg { is_send: false, channel: 11, source: 1, target: 1, seq_no: 0, length: 1 }) ;; stop send data as_coll->inspect
(8.66324ms, 1, GMsg { is_start: true })                                                          ;; work @inspect (combined data)
(8.701874ms, 1, Msg { is_send: true, channel: 13, source: 1, target: 1, seq_no: 0, length: 1 })  ;; start send data inspect->probe
(8.707241ms, 1, GMsg { is_start: false })
(8.770187ms, 1, Sched { id: 10, start_stop: Stop })                                              ;; stop inspect
(8.779691ms, 1, Sched { id: 12, start_stop: Start })                                             ;; start probe
(8.78351ms, 1, Msg { is_send: false, channel: 13, source: 1, target: 1, seq_no: 0, length: 1 })  ;; stop send data inspect->probe
(8.794782ms, 1, Sched { id: 12, start_stop: Stop })                                              ;; stop probe
(8.811369ms, 1, Progress { is_send: true, source: 1, channel: 14, seq_no: 1, addr: [0]})         ;; **start own progress comm!**
(8.826659ms, 1, Sched { id: 0, start_stop: Stop })                                               ;; end dataflow

(8.987087ms, 1, Sched { id: 0, start_stop: Start })                                              ;; start dataflow
(8.994171ms, 1, Progress { is_send: false, source: 1, channel: 14, seq_no: 1, addr: [0]})        ;; **stop own progress comm!**
(9.115763ms, 1, PushProgress { op_id: 6 })                                                       ;; epoch has progressed to red
(9.116423ms, 1, Sched { id: 6, start_stop: Start })                                              ;; wrap up reduce
(9.238919ms, 1, Sched { id: 6, start_stop: Stop })                                               ;; (nothing to do)
(9.247686ms, 1, PushProgress { op_id: 8 })                                                       ;; epoch has progressed to as_coll
(9.248172ms, 1, Sched { id: 8, start_stop: Start })                                              ;; wrap up as_coll
(9.303453ms, 1, Sched { id: 8, start_stop: Stop })                                               ;; (nothing to do)
(9.310637ms, 1, PushProgress { op_id: 10 })                                                      ;; epoch has progressed to inspect
(9.311109ms, 1, Sched { id: 10, start_stop: Start })                                             ;; wrap up inspect
(9.381781ms, 1, Sched { id: 10, start_stop: Stop })                                              ;; (nothing to do)
(9.389307ms, 1, PushProgress { op_id: 12 })                                                      ;; epoch has progressed to probe
(9.389808ms, 1, Sched { id: 12, start_stop: Start })                                             ;; wrap up probe
(9.402946ms, 1, Sched { id: 12, start_stop: Stop })                                              ;; (nothing to do)
(9.407077ms, 1, Sched { id: 0, start_stop: Stop })                                               ;; stop dataflow

(9.450518ms, 1, Text("[st] closed times before: 1"))                                             ;; end epoch 0
