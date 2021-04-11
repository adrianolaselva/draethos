package target

//
//type pgsqlTarget struct {
//	sync.Mutex
//	targetSpec specs.Target
//	codec      interfaces.CodecInterface
//	queue      *list.List
//	parameters []string
//}
//
//func NewPgsqlTarget(targetSpec specs.Target, codec interfaces.CodecInterface) (*pgsqlTarget, error) {
//	return &pgsqlTarget{
//		targetSpec: targetSpec,
//		codec: codec,
//		queue: list.New(),
//		parameters: []string{
//			"host","port","user","password","sslmode",
//		}}, nil
//}
//
//func (p *pgsqlTarget) Initialize() error {
//	if _, ok := p.targetSpec.TargetSpecs.Configurations["host"].(string); !ok {
//
//	}
//
//	if _, ok := p.targetSpec.TargetSpecs.Configurations["host"].(string); !ok {
//
//	}
//
//	//strCon := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
//	//	p.targetSpec.TargetSpecs.Configurations["host"],
//	//	p.targetSpec.TargetSpecs.Configurations[""],
//	//	p.targetSpec.TargetSpecs.Configurations[""],
//	//	p.targetSpec.TargetSpecs.Configurations[""],
//	//	p.targetSpec.TargetSpecs.Database)
//
//	return nil
//}
//
//func (p *pgsqlTarget) Attach(_ string, data map[string]interface{}) error {
//	k.Lock()
//	defer k.Unlock()
//
//	k.queue.PushBack(data)
//	return nil
//}
//
//func (p *pgsqlTarget) CanFlush() bool {
//	return k.queue.Len() >= k.targetSpec.TargetSpecs.BatchSize
//}
//
//func (p *pgsqlTarget) Flush() error {
//	k.Lock()
//	defer k.Unlock()
//
//	if k.queue.Len() == 0 {
//		return nil
//	}
//
//	zap.S().Debugf(fmt.Sprintf("flush %v events", k.queue.Len()))
//
//	for k.queue.Len() > 0 {
//		e := k.queue.Front()
//
//		value, ok := e.Value.(map[string]interface{})
//		if !ok {
//			k.queue.Remove(e)
//			zap.S().Warnf("failed to deserialize event [%x], waiting messages", e.Value)
//			return nil
//		}
//
//		content, err := k.codec.Serialize(value)
//		if err != nil {
//			return err
//		}
//
//
//
//		k.queue.Remove(e)
//	}
//
//	return nil
//}
//
//func (p *pgsqlTarget) Close() error {
//
//	return nil
//}
