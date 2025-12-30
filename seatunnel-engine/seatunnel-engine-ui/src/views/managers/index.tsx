/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { defineComponent, ref } from 'vue'
import { NDataTable } from 'naive-ui'
import type { DataTableColumns } from 'naive-ui'
import { NLayout, NLayoutContent } from 'naive-ui'
import { managerService } from '@/service/manager'
import type { Monitor } from '@/service/manager/types'
import { useRoute } from 'vue-router'

export default defineComponent({
  setup() {
    const route = useRoute()
    const monitors = ref([] as Monitor[])

    const fetch = async () => {
      let res = await managerService.getMonitors()
      const isMaster = route?.path.endsWith('/master') || false
      res = res.filter((row) => row.isMaster === String(isMaster)) || []
      monitors.value = res
    }
    fetch()

    function createColumns(): DataTableColumns<Monitor> {
      return [
        {
          title: '主机',
          key: 'host'
        },
        {
          title: '端口',
          key: 'port'
        },
        {
          title: '物理内存',
          key: 'physical.memory.total'
        },
        {
          title: '堆内存使用',
          key: 'heap.memory.used'
        }
      ]
    }

    const columns = createColumns()
    return () => (
      <NLayout>
        <NLayoutContent>
          <div class="w-full bg-white p-6 border border-gray-100 rounded-xl">
            <h2 class="font-bold text-2xl pb-6">节点管理</h2>
            <NDataTable
              columns={columns}
              data={monitors.value}
              pagination={false}
              bordered={false}
            />
          </div>
        </NLayoutContent>
      </NLayout>
    )
  }
})
