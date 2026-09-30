// Measurements from Solar roof-mask pixels and independently fitted roof planes.
// No bounding rectangle or living area is used as a roof polygon.
const proj4 = require('proj4');
const FT = 3.280839895;
const SQFT = FT * FT;
const round = n => Math.round(n * 10) / 10;
const finite = n => typeof n === 'number' && Number.isFinite(n);

// Raster classification is intentionally done at cell resolution for accurate
// totals. The map, however, must receive continuous roof lines rather than one
// tiny stair-step polyline per cell edge. Join touching edges by type and apply
// a conservative Douglas-Peucker pass in projected metres. Measurements remain
// based on the unsimplified segments below.
function simplifyPath(points, tolerance) {
  if (points.length <= 2) return points;
  const a=points[0],b=points[points.length-1],dx=b[0]-a[0],dy=b[1]-a[1],denom=dx*dx+dy*dy;
  let index=-1,maxDistance=0;
  for(let i=1;i<points.length-1;i++) {
    const p=points[i],t=denom?Math.max(0,Math.min(1,((p[0]-a[0])*dx+(p[1]-a[1])*dy)/denom)):0;
    const distance=Math.hypot(p[0]-(a[0]+t*dx),p[1]-(a[1]+t*dy));
    if(distance>maxDistance){maxDistance=distance;index=i;}
  }
  if(maxDistance<=tolerance) return [a,b];
  const left=simplifyPath(points.slice(0,index+1),tolerance),right=simplifyPath(points.slice(index),tolerance);
  return left.slice(0,-1).concat(right);
}

function mergeDisplaySegments(segments, resolution) {
  const tolerance=Math.max(Math.abs(resolution[0]),Math.abs(resolution[1]))*.75;
  const minimumLength=Math.max(Math.abs(resolution[0]),Math.abs(resolution[1]))*2;
  const grouped=new Map();
  for(const segment of segments) {
    if(!segment.points||segment.points.length<2)continue;
    if(!grouped.has(segment.kind))grouped.set(segment.kind,[]);
    grouped.get(segment.kind).push(segment);
  }
  const output=[];
  for(const [kind,items] of grouped) {
    const planeGroups=new Map();
    for(const item of items) if(item.interfaceKey) {
      if(!planeGroups.has(item.interfaceKey))planeGroups.set(item.interfaceKey,[]);
      planeGroups.get(item.interfaceKey).push(item);
    }
    const fittedItems=new Set();
    for(const planeItems of planeGroups.values()) {
      const points=planeItems.flatMap(item=>item.points),cx=points.reduce((s,p)=>s+p[0],0)/points.length,cy=points.reduce((s,p)=>s+p[1],0)/points.length;
      let xx=0,yy=0,xy=0;for(const p of points){const x=p[0]-cx,y=p[1]-cy;xx+=x*x;yy+=y*y;xy+=x*y;}
      const angle=.5*Math.atan2(2*xy,xx-yy),ux=Math.cos(angle),uy=Math.sin(angle);
      const projected=points.map(p=>(p[0]-cx)*ux+(p[1]-cy)*uy),lo=Math.min(...projected),hi=Math.max(...projected);
      if(hi-lo>=minimumLength) output.push({kind,points:[[cx+lo*ux,cy+lo*uy],[cx+hi*ux,cy+hi*uy]],lengthFeet:round(planeItems.reduce((s,item)=>s+Number(item.lengthFeet||0),0)),confidence:planeItems.some(item=>item.confidence==='Needs confirmation')?'Needs confirmation':'Auto-calculated — confirm'});
      planeItems.forEach(item=>fittedItems.add(item));
    }
    const pathItems=items.filter(item=>!fittedItems.has(item));
    if(!pathItems.length)continue;
    const nodes=new Map(),edges=[];
    const key=p=>p[0].toFixed(4)+','+p[1].toFixed(4);
    const addNode=(p,edgeIndex)=>{const k=key(p);if(!nodes.has(k))nodes.set(k,{point:p,edges:[]});nodes.get(k).edges.push(edgeIndex);return k;};
    pathItems.forEach(item=>{const edge={a:null,b:null,item,used:false};const index=edges.push(edge)-1;edge.a=addNode(item.points[0],index);edge.b=addNode(item.points[item.points.length-1],index);});
    const walk=(startKey,startEdge)=>{
      const points=[nodes.get(startKey).point];let currentKey=startKey,edgeIndex=startEdge,total=0,confidence='Auto-calculated — confirm';
      while(edgeIndex!=null&&!edges[edgeIndex].used) {
        const edge=edges[edgeIndex];edge.used=true;total+=Number(edge.item.lengthFeet||0);if(edge.item.confidence==='Needs confirmation')confidence='Needs confirmation';
        const nextKey=edge.a===currentKey?edge.b:edge.a;points.push(nodes.get(nextKey).point);currentKey=nextKey;
        const available=nodes.get(currentKey).edges.filter(i=>!edges[i].used);edgeIndex=available.length===1?available[0]:null;
      }
      const simplified=simplifyPath(points,tolerance);
      const displayLength=simplified.slice(1).reduce((sum,p,i)=>sum+Math.hypot(p[0]-simplified[i][0],p[1]-simplified[i][1]),0);
      if(displayLength>=minimumLength||kind!=='ambiguous')output.push({kind,points:simplified,lengthFeet:round(total),confidence});
    };
    for(const [nodeKey,node] of nodes) if(node.edges.length!==2) for(const edgeIndex of node.edges) if(!edges[edgeIndex].used)walk(nodeKey,edgeIndex);
    edges.forEach((edge,index)=>{if(!edge.used)walk(edge.a,index);});
  }
  return output;
}

function measureRaster({ width, height, mask, elevation, origin, resolution, target, planes = [], expectedGroundArea }) {
  const [dx, dy] = resolution;
  const pixelArea = Math.abs(dx * dy);
  if (!(width > 2 && height > 2 && pixelArea > 0 && pixelArea <= 1) || mask.length !== width * height || elevation.length !== mask.length) throw new Error('Unsupported roof raster');
  const position = i => [origin[0] + (i % width + .5) * dx, origin[1] + (Math.floor(i / width) + .5) * dy];
  const valid = i => i >= 0 && i < mask.length && mask[i] > 0 && finite(elevation[i]) && elevation[i] > -9000;
  let seed = -1, distance = Infinity;
  for (let i = 0; i < mask.length; i++) if (valid(i)) {
    const p = position(i), d = Math.hypot(p[0] - target[0], p[1] - target[1]);
    if (d < distance) { seed = i; distance = d; }
  }
  if (seed < 0 || distance > 15) return { measurements: {}, reason: 'No roof mask near the selected building' };
  const included = new Uint8Array(mask.length), queue = [seed]; included[seed] = 1;
  for (let q = 0; q < queue.length; q++) {
    const i = queue[q], x = i % width, y = Math.floor(i / width);
    for (const [nx, ny] of [[x-1,y],[x+1,y],[x,y-1],[x,y+1]]) {
      if (nx < 0 || nx >= width || ny < 0 || ny >= height) continue;
      const j = ny * width + nx;
      if (!included[j] && valid(j)) { included[j] = 1; queue.push(j); }
    }
  }
  if (queue.some(i => i % width === 0 || i % width === width-1 || i < width || i >= width*(height-1))) return { measurements: {}, reason: 'Roof extends outside the imagery window' };
  const groundArea = queue.length * pixelArea;
  if (groundArea < 15 || expectedGroundArea && Math.abs(groundArea / expectedGroundArea - 1) > .2) return { measurements: {}, reason: 'Roof mask and selected building area do not agree' };
  const labels = new Int16Array(mask.length); labels.fill(-1);
  let fitted = 0, totalArea = 0;
  for (const i of queue) {
    const [x,y] = position(i);
    let residual = .8, best = -1;
    planes.forEach((p,k) => {
      if (p.bounds && (x < p.bounds[0]-.5 || x > p.bounds[2]+.5 || y < p.bounds[1]-.5 || y > p.bounds[3]+.5)) return;
      const difference = Math.abs(elevation[i] - (p.z + p.gx*(x-p.x) + p.gy*(y-p.y)));
      if (difference < residual) { residual = difference; best = k; }
    });
    labels[i] = best;
    if (best >= 0) { fitted++; totalArea += pixelArea * Math.hypot(1, planes[best].gx, planes[best].gy); }
  }
  const coverage = fitted / queue.length;
  const measurements = {}, segments = [];
  const put = (name,value,method,confidence='Auto-calculated — confirm') => { measurements[name] = { value:round(value), source:'Google Solar roof mask + elevation + roof planes', confidence, method }; };
  // A materially incomplete plane fit cannot yield trustworthy topology.
  if (coverage < .95) {
    // If BuildingInsights is unavailable, fit small local planes to real DSM
    // samples inside the selected roof mask. Never infer linear edge types here.
    let area=0,usable=0;
    for(const i of queue) {
      const cx=i%width,cy=Math.floor(i/width),points=[];
      for(let oy=-2;oy<=2;oy++)for(let ox=-2;ox<=2;ox++) {
        const x=cx+ox,y=cy+oy,j=y*width+x;
        if(x>=0&&x<width&&y>=0&&y<height&&included[j])points.push([ox*dx,oy*dy,elevation[j]]);
      }
      if(points.length<6)continue;
      const means=[0,1,2].map(k=>points.reduce((sum,p)=>sum+p[k],0)/points.length);
      let xx=0,yy=0,xy=0,xz=0,yz=0;
      for(const p of points){const x=p[0]-means[0],y=p[1]-means[1],z=p[2]-means[2];xx+=x*x;yy+=y*y;xy+=x*y;xz+=x*z;yz+=y*z;}
      const det=xx*yy-xy*xy;if(Math.abs(det)<1e-8)continue;
      const gx=(xz*yy-yz*xy)/det,gy=(yz*xx-xz*xy)/det;
      const residual=Math.sqrt(points.reduce((sum,p)=>sum+(p[2]-means[2]-gx*(p[0]-means[0])-gy*(p[1]-means[1]))**2,0)/points.length);
      if(residual>.4||Math.hypot(gx,gy)>2)continue;
      area+=pixelArea*Math.hypot(1,gx,gy);usable++;
    }
    if(usable===queue.length) {
      put('totalRoofArea',area*SQFT,'Slope-corrected area of actual roof-mask cells using local DSM plane fits');
      measurements.totalRoofArea.source='Google Solar roof mask + elevation (BuildingInsights planes unavailable)';
    }
    return { measurements, reason:'Roof edge types require reliable roof-plane topology; area retained where complete DSM coverage permits it', quality:{planeCoverage:coverage,dsmCoverage:usable/queue.length,maskAreaSquareFeet:round(groundArea*SQFT)} };
  }
  // Omitted pixels are not silently treated as flat roof. Direct Solar area takes precedence.
  if (coverage === 1) put('totalRoofArea',totalArea*SQFT,'Sum of measured mask cell areas corrected by each fitted plane slope');
  // Fill the small number of unlabeled roof-mask cells from neighboring fitted
  // planes. Solar plane boxes and raster masks differ by a few edge pixels on
  // otherwise complete roofs; that mismatch must not blank every category.
  if (coverage >= .95) {
    for (let pass=0;pass<4;pass++) for (const i of queue) if(labels[i]<0) {
      const x=i%width,y=Math.floor(i/width),near=[];
      for(const [ox,oy] of [[-1,0],[1,0],[0,-1],[0,1]]) {
        const nx=x+ox,ny=y+oy;if(nx<0||nx>=width||ny<0||ny>=height)continue;
        const label=labels[ny*width+nx];if(label>=0)near.push(label);
      }
      if(near.length) labels[i]=near.sort((a,b)=>near.filter(v=>v===b).length-near.filter(v=>v===a).length)[0];
    }
  }
  const resolvedCoverage=queue.filter(i=>labels[i]>=0).length/queue.length;
  const lengths = {eaves:0,rakes:0,ridges:0,hips:0,valleys:0,stepFlashing:0,headwallFlashing:0};
  let external=0, unknown=0, ambiguous=0;
  const inside = (x,y) => x>=0 && x<width && y>=0 && y<height && included[y*width+x];
  const edgeGeometry=(i,ox,oy)=>{const [cx,cy]=position(i),halfX=Math.abs(dx)/2,halfY=Math.abs(dy)/2;return ox?[ [cx+ox*halfX,cy-halfY],[cx+ox*halfX,cy+halfY] ]:[ [cx-halfX,cy+oy*halfY],[cx+halfX,cy+oy*halfY] ];};
  const addSegment=(kind,i,ox,oy,length,confidence='Auto-calculated — confirm',interfaceKey=null)=>{const points=edgeGeometry(i,ox,oy);segments.push({kind,points,lengthFeet:round(length*FT),confidence,interfaceKey});};
  for (const i of queue) {
    const x=i%width,y=Math.floor(i/width);
    for (const [ox,oy] of [[-1,0],[1,0],[0,-1],[0,1]]) {
      let a=planes[labels[i]];
      const j=(y+oy)*width+x+ox, edge=[oy ? Math.abs(dx):0, ox ? Math.abs(dy):0];
      if (!inside(x+ox,y+oy)) {
        external += Math.hypot(...edge);
        if (!a || Math.hypot(a.gx,a.gy)<.05) { unknown+=Math.hypot(...edge);continue; }
        // Smoothed mask normals avoid interpreting raster stair steps as roof corners.
        let nx=0,ny=0;
        for(let t=-2;t<=2;t++) { nx+=Number(inside(x-2,y+t))-Number(inside(x+2,y+t)); ny+=Number(inside(x+t,y-2))-Number(inside(x+t,y+2)); }
        nx*=Math.sign(dx);ny*=Math.sign(dy);
        const straight=[-1,1].some(direction=>[1,2,3,4].every(k=>inside(x+oy*k*direction,y+ox*k*direction)&&!inside(x+ox+oy*k*direction,y+oy+ox*k*direction)));
        if(straight){nx=ox*Math.sign(dx);ny=oy*Math.sign(dy);}
        // At a hip corner two planes can fit the same pixel. Select the plane
        // facing this exterior edge rather than treating the tie as a rake.
        const [px,py]=position(i);
        for(const candidate of planes) {
          const residual=Math.abs(elevation[i]-(candidate.z+candidate.gx*(px-candidate.x)+candidate.gy*(py-candidate.y)));
          const facing=-candidate.gx*nx-candidate.gy*ny;
          if(residual<.02 && facing>-a.gx*nx-a.gy*ny && (!candidate.bounds||(px>=candidate.bounds[0]&&px<=candidate.bounds[2]&&py>=candidate.bounds[1]&&py<=candidate.bounds[3]))) a=candidate;
        }
        const norm=Math.hypot(nx,ny),slope=Math.hypot(a.gx,a.gy);
        if(!norm) {unknown+=Math.hypot(...edge);continue;}
        const dot=(-a.gx*nx-a.gy*ny)/(slope*norm);
        let kind,tx,ty;
        if(dot>.85){kind='eaves';tx=-a.gy/slope;ty=a.gx/slope;}
        else if(Math.abs(dot)<.35){kind='rakes';tx=a.gx/slope;ty=a.gy/slope;}
        else {unknown+=Math.hypot(...edge);continue;}
        const horizontal=Math.abs(edge[0]*tx)+Math.abs(edge[1]*ty);
        const measured=horizontal*Math.hypot(1,a.gx*tx+a.gy*ty);
        lengths[kind]+=measured;addSegment(kind,i,ox,oy,measured);
      } else if (j>i && labels[j]!==labels[i]) {
        const b=planes[labels[j]];
        if(!a||!b){ambiguous++;continue;}
        const [px,py]=position(i),za=a.z+a.gx*(px-a.x)+a.gy*(py-a.y),zb=b.z+b.gx*(px-b.x)+b.gy*(py-b.y);
        const normal=[ox*Math.sign(dx),oy*Math.sign(dy)];
        if(Math.abs(za-zb)>1){
          const lower=za<zb?a:b,slopeNorm=Math.hypot(lower.gx,lower.gy);
          const tangent=[-normal[1],normal[0]],alignment=slopeNorm?Math.abs((tangent[0]*lower.gx+tangent[1]*lower.gy)/slopeNorm):.5;
          const horizontal=Math.hypot(ox?Math.abs(dy):Math.abs(dx));
          const alongSlope=lower.gx*tangent[0]+lower.gy*tangent[1];
          const measured=horizontal*Math.hypot(1,alongSlope);
          const kind=alignment>=.65?'stepFlashing':alignment<=.35?'headwallFlashing':null;
          const interfaceKey='planes:'+Math.min(labels[i],labels[j])+':'+Math.max(labels[i],labels[j]);
          if(kind){lengths[kind]+=measured;addSegment(kind,i,ox,oy,measured,'Auto-calculated — confirm',interfaceKey);}else{ambiguous++;addSegment('ambiguous',i,ox,oy,horizontal,'Needs confirmation',interfaceKey);}
          continue;
        }
        const curvature=(a.gx-b.gx)*normal[0]+(a.gy-b.gy)*normal[1];
        let tx=-(a.gy-b.gy),ty=a.gx-b.gx,norm=Math.hypot(tx,ty);
        if(norm<.05){ambiguous++;continue;}
        tx/=norm;ty/=norm;
        const slope=a.gx*tx+a.gy*ty;
        const kind=curvature>.03 ? (Math.abs(slope)<.08?'ridges':'hips') : curvature<-.03 ? 'valleys' : null;
        const interfaceKey='planes:'+Math.min(labels[i],labels[j])+':'+Math.max(labels[i],labels[j]);
        if(!kind){ambiguous++;addSegment('ambiguous',i,ox,oy,Math.hypot(...edge),'Needs confirmation',interfaceKey);continue;}
        const measured=(Math.abs(edge[0]*tx)+Math.abs(edge[1]*ty))*Math.hypot(1,slope);
        lengths[kind]+=measured;addSegment(kind,i,ox,oy,measured,'Auto-calculated — confirm',interfaceKey);
      }
    }
  }
  const quality={planeCoverage:coverage,resolvedPlaneCoverage:resolvedCoverage,unclassifiedPerimeterFraction:external?unknown/external:1,ambiguousSegmentCount:ambiguous,maskAreaSquareFeet:round(groundArea*SQFT)};
  for(const kind of ['eaves','rakes']) put(kind,lengths[kind]*FT,'Classified roof-mask perimeter projected onto fitted roof-plane edge directions');
  for(const kind of ['ridges','hips','valleys']) put(kind,lengths[kind]*FT,'Classified intersections of adjacent fitted roof planes; slope-corrected length');
  for(const kind of ['stepFlashing','headwallFlashing']) if(lengths[kind]>0) put(kind,lengths[kind]*FT,'Roof-to-wall elevation breaks classified by line orientation to the lower roof plane; slope-corrected length');
  return {measurements,segments:mergeDisplaySegments(segments,resolution),quality,reason:ambiguous?'Some roof interfaces require confirmation':null};
}

async function rasterMeasurements({layerData,insight,key,fetch,latitude,longitude}) {
  const {fromArrayBuffer}=await import('geotiff');
  async function read(url) {
    const parsed=new URL(url);
    if(parsed.protocol!=='https:'||parsed.hostname!=='solar.googleapis.com')throw new Error('Unexpected Solar raster host');
    const response=await fetch(parsed,{headers:{'X-Goog-Api-Key':key},signal:AbortSignal.timeout(15000)});
    if(!response.ok)throw new Error('Solar raster HTTP '+response.status);
    const bytes=await response.arrayBuffer();if(bytes.byteLength>32000000)throw new Error('Solar raster too large');
    const image=await (await fromArrayBuffer(bytes)).getImage();
    if(image.getWidth()*image.getHeight()>1500000)throw new Error('Solar raster resolution exceeds processing limit');
    return {image,values:(await image.readRasters({samples:[0]}))[0]};
  }
  const [mask,dsm]=await Promise.all([read(layerData.maskUrl),read(layerData.dsmUrl)]);
  // Solar uses ModelTransformation: its row coefficient already has the signed
  // northing direction. getResolution() reverses that sign for this TIFF form.
  function grid(image) {
    const transform=image.fileDirectory.getValue('ModelTransformation');
    if(transform) {
      if(transform[1]!==0||transform[4]!==0)throw new Error('Rotated Solar raster grid is unsupported');
      return {origin:[transform[3],transform[7],transform[11]],resolution:[transform[0],transform[5],transform[10]]};
    }
    return {origin:image.getOrigin(),resolution:image.getResolution()};
  }
  const {origin,resolution}=grid(mask.image),dsmGrid=grid(dsm.image);
  if(mask.image.getWidth()!==dsm.image.getWidth()||mask.image.getHeight()!==dsm.image.getHeight()||origin.some((n,i)=>Math.abs(n-dsmGrid.origin[i])>.01)||resolution.some((n,i)=>Math.abs(n-dsmGrid.resolution[i])>.001))throw new Error('Solar mask and elevation grids are not aligned');
  const code=mask.image.getGeoKeys().ProjectedCSTypeGeoKey;
  let projection;
  if(code>=32601&&code<=32660)projection='+proj=utm +zone='+(code-32600)+' +datum=WGS84 +units=m +no_defs';
  else if(code>=32701&&code<=32760)projection='+proj=utm +zone='+(code-32700)+' +south +datum=WGS84 +units=m +no_defs';
  else throw new Error('Unsupported Solar raster projection');
  const xy=p=>proj4('EPSG:4326',projection,[p.longitude,p.latitude]);
  const ll=p=>{const value=proj4(projection,'EPSG:4326',p);return {longitude:value[0],latitude:value[1]};};
  const raw=insight?.solarPotential?.roofSegmentStats||[];
  const planes=raw.filter(p=>p.center&&finite(p.planeHeightAtCenterMeters)&&finite(p.pitchDegrees)&&finite(p.azimuthDegrees)).map(p=>{
    const center=xy(p.center),pitch=p.pitchDegrees*Math.PI/180,azimuth=p.azimuthDegrees*Math.PI/180;
    const lo=p.boundingBox?xy(p.boundingBox.sw):null,hi=p.boundingBox?xy(p.boundingBox.ne):null;
    return {x:center[0],y:center[1],z:p.planeHeightAtCenterMeters,gx:-Math.tan(pitch)*Math.sin(azimuth),gy:-Math.tan(pitch)*Math.cos(azimuth),bounds:lo&&hi?[Math.min(lo[0],hi[0]),Math.min(lo[1],hi[1]),Math.max(lo[0],hi[0]),Math.max(lo[1],hi[1])]:null};
  });
  const result=measureRaster({width:mask.image.getWidth(),height:mask.image.getHeight(),mask:mask.values,elevation:dsm.values,origin,resolution,target:xy(insight?.center||{latitude,longitude}),planes,expectedGroundArea:insight?.solarPotential?.wholeRoofStats?.groundAreaMeters2});
  result.segments=(result.segments||[]).map(segment=>Object.assign({},segment,{points:segment.points.map(ll)}));
  return result;
}
module.exports={measureRaster,rasterMeasurements,mergeDisplaySegments};
