from pathlib import Path
import importlib.util,json,subprocess,sys
root=Path.cwd();output=Path(sys.argv[1]).resolve()
sys.path.insert(0,str(root/'tools'))
spec=importlib.util.spec_from_file_location('stage8_profile',root/'tools/benchmark_stage8.py');b=importlib.util.module_from_spec(spec);spec.loader.exec_module(b)
reference=json.loads((root/'artifacts/stage8/baseline-20261010/summary.json').read_text());reference['configuration'].update(seconds=12,repeats=1,clients='32');reference['trial_order']=[['kedr',32,1]]
ref=Path('/tmp/kedr-stage8-profile-reference.json');ref.write_text(json.dumps(reference))
original=b.trial
def profiled(parameters,clients,repeat,args,server_pid,path):
 with (args.output/'sample-command-log.txt').open('w') as log:
  sampler=subprocess.Popen(['/usr/bin/sample',str(server_pid),'5','1','-file',str(args.output/'server-sample.txt')],stdout=log,stderr=subprocess.STDOUT)
  try:return original(parameters,clients,repeat,args,server_pid,path)
  finally:
   sampler.wait(timeout=20)
   if sampler.returncode:raise RuntimeError('sampler failed')
b.trial=profiled
sys.argv=['benchmark_stage8.py','--engines','kedr','--reference',str(ref),'--output',str(output),'--seconds','12','--repeats','1','--clients','32']
b.main();p=output/'summary.json';d=json.loads(p.read_text());d['kind']='instrumented-profile-not-a-performance-comparison';d['profiling']={'command':'sample PID 5 1','duration_seconds':5,'sample_interval_ms':1,'purpose':'hotspot diagnosis; do not compare instrumented TPS to ordinary trials'};p.write_text(json.dumps(d,ensure_ascii=False,indent=2)+'\n')
