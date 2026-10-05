"""Exécute le JS livré : attente du POST, doubles clics, retour identique et erreur réseau."""
from pathlib import Path
import shutil
import subprocess

import pytest


@pytest.mark.parametrize("scenario", ["succes", "identique", "erreur"])
def test_etat_pendant_traitement_et_retour(scenario):
    node = shutil.which("node")
    if not node:
        pytest.skip("Node nécessaire pour exécuter ce test JavaScript")
    script = Path(__file__).parents[1] / "app/static/js/cloture_actualisation.js"
    harness = r"""
const fs = require('node:fs'), vm = require('node:vm'), assert = require('node:assert/strict');
const scenario = process.argv[2];
const retour = '/clotures/mois/2026-09?actualisation=succes&module_actualise=MENAGES#module-menages';
let submits = [], fetches = [], reloaded = 0, replaced = [], focussed = 0, scrolled = 0;
let finish;
function element() {
  return {attrs:{}, classList:{add(){},remove(){}},
    setAttribute(k,v){this.attrs[k]=v;},getAttribute(k){return this.attrs[k]??null;},removeAttribute(k){delete this.attrs[k];},
    focus(){focussed++;}};
}
const button = Object.assign(element(), {innerHTML:'Actualiser',textContent:'Actualiser',disabled:false});
const autre = Object.assign(element(), {disabled:false});
const feedback = Object.assign(element(), {hidden:true,textContent:''});
const titre = element();
const carte = Object.assign(element(), {querySelector(s){return s==='h3' ? titre : feedback;},
  scrollIntoView(){scrolled++;}});
const form = {action:'http://localhost/clotures/mois/2026-09/modules/MENAGES/actualiser',
  querySelector(){return button;},closest(){return carte;},addEventListener(e,cb){submits.push(cb);}};
const form2 = {querySelector(){return autre;},addEventListener(){}};
let onShow;
const loc = {href:'http://localhost'+(scenario==='identique' ? retour : '/clotures/mois/2026-09'),
  search:'',hash:'#module-menages',reload(){reloaded++;},replace(u){replaced.push(u);}};
const context = {URL, URLSearchParams, location:loc,
  document:{querySelectorAll(){return [form,form2];},getElementById(){return carte;}},
  window:{addEventListener(e,cb){onShow=cb;}},
  fetch(u,opts){fetches.push({u,opts}); return new Promise(r=>{finish=r;});}};
vm.runInNewContext(fs.readFileSync(process.argv[1],'utf8'),context);
(async()=>{
  const ev = {preventDefault(){}};
  const pending = submits[0](ev);
  assert.equal(fetches.length,1);
  assert.equal(fetches[0].opts.method,'POST');
  assert.equal(fetches[0].opts.headers.Accept,'application/json');
  assert.equal(button.textContent,'Actualisation…');
  assert.equal(button.attrs['aria-busy'],'true');
  assert.equal(button.attrs['aria-label'],'Actualisation en cours…');
  assert.equal(button.disabled,true); assert.equal(autre.disabled,true);
  assert.equal(feedback.hidden,false); assert.equal(feedback.textContent,'Actualisation en cours…');
  await submits[0](ev);
  assert.equal(fetches.length,1,'un second clic ne doit pas envoyer un second POST');
  finish({ok:scenario!=='erreur',json:async()=>({retour})});
  await pending;
  if(scenario==='identique') {
    assert.equal(reloaded,1,'une URL identique exige une vraie relecture GET');
    assert.equal(replaced.length,0);
  } else if(scenario==='succes') {
    assert.deepEqual(replaced,[retour]); assert.equal(reloaded,0);
  } else {
    assert.equal(button.disabled,false); assert.equal(autre.disabled,false);
    assert.equal(button.attrs['aria-busy'],undefined);
    assert.equal(feedback.attrs.role,'alert'); assert.match(feedback.textContent,/n'a pas pu aboutir/);
    assert.equal(focussed,1); assert.equal(replaced.length,0); assert.equal(reloaded,0);
  }
  loc.search='?module_actualise=MENAGES'; onShow();
  assert.equal(scrolled,1); assert.ok(focussed>=1);
})().catch(e=>{console.error(e);process.exitCode=1;});
"""
    result = subprocess.run([node, "-e", harness, str(script), scenario],
                            capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stdout + result.stderr
